"""从链尾上传新导出的同尺寸 RAW 镜像，追加一个恢复点。

处理流程
--------
1. `begin_upload`：管理员在页面选定链尾后先登记一笔上传任务。此时把所选
   恢复点的**有效内容**（沿链折叠后的逐槽指针快照）写入 `upload_parent_blocks`，
   之后逐块比对只查这一张表，既不把整盘载入内存，也不受接收期间清退影响
   —— 清退只改指针，不改存活恢复点的有效内容。
2. `receive_and_commit`：请求体逐块读取（每次只持有一个块大小的缓冲），
   每块算出 SHA-256 与父点同槽比对：
     * 与父点相同  → 不写指针，入链后自然继承；
     * 不同且全零  → 写 `is_zero=1` 的显式清零指针（不存块内容）；
     * 其余        → 内容暂存到 `upload_contents`，写普通指针。
   暂存数据每块提交一次，让清退发布在接收全程都能拿到写锁。
3. `commit_upload`：`BEGIN IMMEDIATE` 内复核“所选恢复点仍是链尾”，然后把
   暂存内容并入 `blocks`、恢复点与指针一次性入链。任何一步失败都会把本次
   暂存区全部删除，`backups` / `blocks` 不留本次恢复点或新块。

为什么提交时只校验“仍是链尾”
------------------------------
清退中间恢复点会重写后继指针，但会保持后继的有效内容逐字节不变；因此接收
期间发生清退不会破坏逐块比对结果。只有“又有一个恢复点接在了所选点之后”才
会让这批镜像不再是从链尾增量，需要管理员重选链尾。
"""
from __future__ import annotations

import hashlib
import sqlite3
from uuid import uuid4

from .chain import (
    ZERO_HASH,
    ConflictError,
    DomainError,
    LengthMismatchError,
    NotFoundError,
    _now,
    chain_version,
    disk_spec,
    get_backup,
    set_meta,
    VERSION_KEY,
)

# 每处理完一块立即提交并释放写锁，确保阻塞等待网络数据时不持有写事务，
# 中间恢复点的清退发布在接收全程都能照常拿到写锁。
FLUSH_EVERY_BLOCKS = 1


def get_upload_job(conn: sqlite3.Connection, job_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM upload_jobs WHERE id = ?", (job_id,)).fetchone()
    if row is None:
        raise NotFoundError(f"上传任务不存在: {job_id}")
    return row


def list_upload_jobs(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            "SELECT id, parent_id, parent_seq, state, filename, label, "
            "expected_size, received_bytes, image_sha256, result_backup_id, "
            "created_at, committed_at, error FROM upload_jobs "
            "ORDER BY created_at DESC"
        )
    )


def _tail(conn: sqlite3.Connection) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM backups ORDER BY seq DESC LIMIT 1").fetchone()
    if row is None:
        raise DomainError("链为空，无法追加恢复点")
    return row


# 沿链折叠出某恢复点的有效内容：每个槽位取 seq 不大于该点的最新一条指针。
# 用窗口函数在 SQL 内完成，逐行流式返回，不把整张指针表读进内存。
EFFECTIVE_SQL = """
SELECT block_index, block_hash, is_zero FROM (
    SELECT bb.block_index, bb.block_hash, bb.is_zero,
           ROW_NUMBER() OVER (
               PARTITION BY bb.block_index ORDER BY b.seq DESC
           ) AS rn
    FROM backup_blocks bb
    JOIN backups b ON b.id = bb.backup_id
    WHERE b.seq <= ?
) WHERE rn = 1 ORDER BY block_index
"""


def _snapshot_parent(conn: sqlite3.Connection, job_id: str, parent: sqlite3.Row) -> None:
    """把父点的有效内容快照进 upload_parent_blocks。

    折叠在 SQL 内完成（窗口函数），不会为整盘建 Python 字典；快照本身也是
    逐行写入库，内存占用与磁盘大小无关。之后逐块比对只查这一张表。
    """
    conn.execute(
        "INSERT INTO upload_parent_blocks(job_id, block_index, block_hash, is_zero) "
        "SELECT ?, block_index, block_hash, is_zero FROM (" + EFFECTIVE_SQL + ")",
        (job_id, parent["seq"]),
    )
    _, block_count = disk_spec(conn)
    have = conn.execute(
        "SELECT COUNT(*) AS c FROM upload_parent_blocks WHERE job_id = ?", (job_id,)
    ).fetchone()["c"]
    if have != block_count:
        raise DomainError(
            f"所选恢复点 P{parent['seq']} 的槽位不完整：应有 {block_count} 块，"
            f"实际解析出 {have} 块"
        )


def _fingerprint(conn: sqlite3.Connection, job_id: str) -> str:
    """对快照逐行哈希，得到父点有效内容的指纹。"""
    h = hashlib.sha256()
    for r in conn.execute(
        "SELECT block_index, block_hash, is_zero FROM upload_parent_blocks "
        "WHERE job_id = ? ORDER BY block_index",
        (job_id,),
    ):
        h.update(f"{r['block_index']}:{r['block_hash']}:{r['is_zero']};".encode("ascii"))
    return h.hexdigest()


def _effective_fingerprint(conn: sqlite3.Connection, backup_id: str) -> str:
    """沿链现算父点有效内容的指纹（提交时复核用）。"""
    row = conn.execute("SELECT seq FROM backups WHERE id = ?", (backup_id,)).fetchone()
    if row is None:
        raise NotFoundError(f"恢复点不存在: {backup_id}")
    h = hashlib.sha256()
    for r in conn.execute(EFFECTIVE_SQL, (row["seq"],)):
        h.update(f"{r['block_index']}:{r['block_hash']}:{r['is_zero']};".encode("ascii"))
    return h.hexdigest()


def begin_upload(
    conn: sqlite3.Connection,
    parent_id: str,
    filename: str,
    label: str,
    declared_size: int | None,
) -> dict:
    """登记一笔上传。只接受当前链尾作为父点。"""
    block_size, block_count = disk_spec(conn)
    expected = block_size * block_count

    parent = get_backup(conn, parent_id)  # 不存在 → 404
    if declared_size is not None and declared_size != expected:
        raise LengthMismatchError(
            f"镜像长度不符：磁盘为 {block_count} 块 × {block_size}B = {expected}B，"
            f"所选文件为 {declared_size}B"
        )
    tail = _tail(conn)
    if parent["id"] != tail["id"]:
        raise ConflictError(
            f"所选恢复点 P{parent['seq']} 不是链尾（当前链尾是 P{tail['seq']}），"
            "请刷新后重新选择链尾再上传"
        )

    label = (label or "").strip() or f"上传镜像 {filename or ''}".strip()
    filename = filename or "upload.img"

    job_id = uuid4().hex
    try:
        conn.execute(
            "INSERT INTO upload_jobs(id, parent_id, parent_seq, parent_fingerprint, "
            "based_on_version, expected_size, filename, label, state, created_at, "
            "received_bytes) VALUES(?, ?, ?, '', ?, ?, ?, ?, 'open', ?, 0)",
            (
                job_id,
                parent["id"],
                parent["seq"],
                chain_version(conn),
                expected,
                filename,
                label,
                _now(),
            ),
        )
        _snapshot_parent(conn, job_id, parent)
        fingerprint = _fingerprint(conn, job_id)
        conn.execute(
            "UPDATE upload_jobs SET parent_fingerprint = ? WHERE id = ?",
            (fingerprint, job_id),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {
        "upload_id": job_id,
        "parent_id": parent["id"],
        "parent_seq": parent["seq"],
        "expected_size": expected,
        "block_size": block_size,
        "block_count": block_count,
        "state": "open",
    }


# --------------------------------------------------------------- receive ---
class _Reader:
    """逐块读取请求体；任意时刻最多缓冲一个块。"""

    def __init__(self, stream, block_size: int, image_hasher):
        self.stream = stream
        self.block_size = block_size
        self.image_hasher = image_hasher
        self.total = 0

    def read_block(self) -> bytes:
        remaining = self.block_size
        parts: list[bytes] = []
        while remaining:
            try:
                chunk = self.stream.read(remaining)
            except Exception as exc:
                # 客户端断开/连接被重置：werkzeug 会在读流时抛 ClientDisconnected
                # 或 BadRequest。统一按“上传中断”处理并给出可读原因。
                raise LengthMismatchError(
                    f"上传中断：已收到 {self.total} 字节（{exc.__class__.__name__}）"
                ) from exc
            if not chunk:
                raise LengthMismatchError(
                    f"上传中断/长度不足：已收到 {self.total} 字节，"
                    f"还差 {remaining} 字节才够一块"
                )
            parts.append(chunk)
            remaining -= len(chunk)
            self.total += len(chunk)
        block = b"".join(parts)
        self.image_hasher.update(block)
        return block


def receive_and_commit(conn: sqlite3.Connection, job_id: str, stream) -> dict:
    """逐块接收镜像；完整收齐且链尾未变时原子追加恢复点。"""
    job = get_upload_job(conn, job_id)
    if job["state"] != "open":
        raise ConflictError(f"上传任务状态为 {job['state']}，不能再接收数据")

    block_size, block_count = disk_spec(conn)
    expected = block_size * block_count
    if job["expected_size"] != expected:
        abort_upload(conn, job_id, "磁盘尺寸已变化，本次上传作废")
        raise ConflictError("磁盘尺寸已变化，请重新开始上传")

    zero_block = b"\x00" * block_size
    image_hasher = hashlib.sha256()
    reader = _Reader(stream, block_size, image_hasher)
    stats = {"inherited": 0, "written": 0, "zeroed": 0, "new_contents": 0}

    try:
        parent_count = conn.execute(
            "SELECT COUNT(*) AS c FROM upload_parent_blocks WHERE job_id = ?", (job_id,)
        ).fetchone()["c"]
        if parent_count != block_count:
            raise DomainError("父点快照不完整，请重新开始上传")

        for i in range(block_count):
            block = reader.read_block()

            # 管理员可能在另一连接上点了取消；每块复核一次状态，一旦作废
            # 立即停手，不再往暂存区写任何新行。
            state_row = conn.execute(
                "SELECT state, error FROM upload_jobs WHERE id = ?", (job_id,)
            ).fetchone()
            if state_row is None or state_row["state"] != "open":
                raise ConflictError(
                    state_row["error"] if state_row and state_row["error"]
                    else f"上传任务状态为 {state_row['state'] if state_row else 'missing'}，已停止接收"
                )

            sha = hashlib.sha256(block).hexdigest()
            prow = conn.execute(
                "SELECT block_hash, is_zero FROM upload_parent_blocks "
                "WHERE job_id = ? AND block_index = ?",
                (job_id, i),
            ).fetchone()
            if prow is None:  # pragma: no cover - 上面已校验块数
                raise DomainError(f"父点快照缺少槽位 {i}，请重新开始上传")
            parent_hash, parent_zero = prow["block_hash"], prow["is_zero"]

            if parent_zero:
                identical = block == zero_block
            else:
                identical = sha == parent_hash

            if identical:
                stats["inherited"] += 1
                continue

            if block == zero_block:
                conn.execute(
                    "INSERT INTO upload_pointers(job_id, block_index, block_hash, is_zero) "
                    "VALUES(?, ?, ?, 1) "
                    "ON CONFLICT(job_id, block_index) DO UPDATE SET "
                    "block_hash = excluded.block_hash, is_zero = 1",
                    (job_id, i, ZERO_HASH),
                )
                stats["zeroed"] += 1
            else:
                exists = conn.execute(
                    "SELECT 1 FROM blocks WHERE hash = ?", (sha,)
                ).fetchone()
                if not exists:
                    already = conn.execute(
                        "SELECT 1 FROM upload_contents WHERE job_id = ? AND hash = ?",
                        (job_id, sha),
                    ).fetchone()
                    if not already:
                        conn.execute(
                            "INSERT INTO upload_contents(job_id, hash, size, content) "
                            "VALUES(?, ?, ?, ?)",
                            (job_id, sha, len(block), block),
                        )
                        stats["new_contents"] += 1
                conn.execute(
                    "INSERT INTO upload_pointers(job_id, block_index, block_hash, is_zero) "
                    "VALUES(?, ?, ?, 0) "
                    "ON CONFLICT(job_id, block_index) DO UPDATE SET "
                    "block_hash = excluded.block_hash, is_zero = 0",
                    (job_id, i, sha),
                )
                stats["written"] += 1

            if (i + 1) % FLUSH_EVERY_BLOCKS == 0:
                conn.execute(
                    "UPDATE upload_jobs SET received_bytes = ? WHERE id = ?",
                    (reader.total, job_id),
                )
                conn.commit()

        # 读到此处仍可能已被取消：提交前再看一眼任务状态
        state_row = conn.execute(
            "SELECT state, error FROM upload_jobs WHERE id = ?", (job_id,)
        ).fetchone()
        if state_row is None or state_row["state"] != "open":
            raise ConflictError(
                state_row["error"] if state_row and state_row["error"] else "上传任务已作废"
            )

        # 多读一个字节：长度超限要整笔作废，不能悄悄截断
        extra = stream.read(1)
        if extra:
            raise LengthMismatchError(
                f"镜像长度不符：磁盘应为 {expected}B，上传内容超出该长度"
            )

        conn.execute(
            "UPDATE upload_jobs SET received_bytes = ? WHERE id = ?",
            (reader.total, job_id),
        )
        conn.commit()

        image_sha = image_hasher.hexdigest()
        summary = commit_upload(conn, job_id, image_sha, reader.total)
        summary.update(stats)
        return summary
    except ConflictError as exc:
        # 可能是 commit_upload 判定“链尾已变”后已自行标记 aborted，也可能是
        # 管理员在页面上点了取消（另一连接把任务置为 aborted）。两种情况下
        # 都再走一次 abort 以清掉可能刚写进去的暂存行；已提交的则绝不动。
        if conn.in_transaction:
            conn.rollback()
        job = get_upload_job(conn, job_id)
        if job["state"] != "committed":
            abort_upload(conn, job_id, job["error"] or str(exc))
        raise
    except (DomainError, LengthMismatchError) as exc:
        abort_upload(conn, job_id, str(exc))
        raise
    except Exception as exc:  # 客户端断开、磁盘错误等
        abort_upload(conn, job_id, f"接收中断: {exc}")
        raise


def commit_upload(
    conn: sqlite3.Connection, job_id: str, image_sha256: str, received: int
) -> dict:
    """原子地把暂存区转正：暂存内容 → blocks，指针 + 恢复点一次入链。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        job = get_upload_job(conn, job_id)
        if job["state"] != "open":
            raise ConflictError(f"上传任务状态为 {job['state']}，不能提交")

        if received != job["expected_size"]:
            raise LengthMismatchError(
                f"镜像长度不符：应为 {job['expected_size']}B，实际 {received}B"
            )

        tail = _tail(conn)
        if tail["id"] != job["parent_id"]:
            conn.execute(
                "UPDATE upload_jobs SET state = 'aborted', error = ?, received_bytes = ? "
                "WHERE id = ?",
                (
                    f"提交时链尾已变：所选 P{job['parent_seq']}，当前链尾 P{tail['seq']}",
                    received,
                    job_id,
                ),
            )
            _clear_staging(conn, job_id)
            conn.commit()
            raise ConflictError(
                f"所选恢复点 P{job['parent_seq']} 已不是链尾（当前链尾是 P{tail['seq']}），"
                "请刷新页面、重新选择链尾后再上传"
            )

        block_size, block_count = disk_spec(conn)

        # 防御性复核：脚本按“登记时父点有效内容”做的继承判定。清退只会保持
        # 存活点的有效内容不变，所以正常情况下指纹必然一致；若不一致说明父点
        # 内容被并发改写，继续入链会让还原结果与上传不一致，宁可作废。
        if _effective_fingerprint(conn, job["parent_id"]) != job["parent_fingerprint"]:
            conn.execute(
                "UPDATE upload_jobs SET state = 'aborted', error = ?, received_bytes = ? "
                "WHERE id = ?",
                ("提交时父点有效内容已变化", received, job_id),
            )
            _clear_staging(conn, job_id)
            conn.commit()
            raise ConflictError(
                "所选恢复点的内容在接收期间发生了变化，本次镜像无法与链拼接，请重试"
            )

        # 暂存内容并入 blocks；同一内容可能已因哈希共享存在，OR IGNORE 即可
        conn.execute(
            "INSERT OR IGNORE INTO blocks(hash, size, is_zero, content) "
            "SELECT hash, size, 0, content FROM upload_contents WHERE job_id = ?",
            (job_id,),
        )
        # 显式清零标记块（种子库已有；旧库升级也补上）
        conn.execute(
            "INSERT OR IGNORE INTO blocks(hash, size, is_zero, content) "
            "VALUES(?, 0, 1, ?)",
            (ZERO_HASH, b""),
        )

        seq = conn.execute("SELECT COALESCE(MAX(seq), -1) + 1 AS s FROM backups").fetchone()["s"]
        backup_id = uuid4().hex
        conn.execute(
            "INSERT INTO backups(id, seq, kind, label, created_at) VALUES(?, ?, 'inc', ?, ?)",
            (backup_id, seq, job["label"], _now()),
        )
        conn.executemany(
            "INSERT INTO backup_blocks(backup_id, block_index, block_hash, is_zero) "
            "VALUES(?, ?, ?, ?)",
            [
                (backup_id, r["block_index"], r["block_hash"], r["is_zero"])
                for r in conn.execute(
                    "SELECT block_index, block_hash, is_zero FROM upload_pointers "
                    "WHERE job_id = ? ORDER BY block_index",
                    (job_id,),
                )
            ],
        )

        version = chain_version(conn) + 1
        set_meta(conn, VERSION_KEY, str(version))
        conn.execute(
            "UPDATE upload_jobs SET state = 'committed', committed_at = ?, "
            "result_backup_id = ?, image_sha256 = ?, received_bytes = ? WHERE id = ?",
            (_now(), backup_id, image_sha256, received, job_id),
        )
        _clear_staging(conn, job_id)
        conn.commit()
        return {
            "upload_id": job_id,
            "state": "committed",
            "backup_id": backup_id,
            "seq": seq,
            "kind": "inc",
            "label": job["label"],
            "parent_id": job["parent_id"],
            "parent_seq": job["parent_seq"],
            "image_sha256": image_sha256,
            "size": received,
            "block_size": block_size,
            "block_count": block_count,
            "chain_version": version,
        }
    except ConflictError:
        if conn.in_transaction:
            conn.rollback()
        raise
    except DomainError:
        if conn.in_transaction:
            conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise


def _clear_staging(conn: sqlite3.Connection, job_id: str) -> None:
    conn.execute("DELETE FROM upload_contents WHERE job_id = ?", (job_id,))
    conn.execute("DELETE FROM upload_pointers WHERE job_id = ?", (job_id,))
    conn.execute("DELETE FROM upload_parent_blocks WHERE job_id = ?", (job_id,))


def abort_upload(conn: sqlite3.Connection, job_id: str, reason: str) -> dict:
    """作废一笔上传并清掉它的全部暂存数据；链与块内容不受影响。"""
    if conn.in_transaction:
        conn.rollback()
    conn.execute("BEGIN IMMEDIATE")
    try:
        job = get_upload_job(conn, job_id)
        if job["state"] == "committed":
            conn.commit()
            raise ConflictError("该上传已提交入链，不能作废")
        conn.execute(
            "UPDATE upload_jobs SET state = 'aborted', error = ? WHERE id = ?",
            (reason, job_id),
        )
        _clear_staging(conn, job_id)
        conn.commit()
        return {"upload_id": job_id, "state": "aborted"}
    except ConflictError:
        if conn.in_transaction:
            conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise


def recover_pending_uploads(conn: sqlite3.Connection) -> int:
    """进程重启后调用：未完成的上传一律作废并清掉暂存数据。

    同时清扫所有非 open 任务残留的暂存行：若管理员在接收途中点了取消，
    仍在逐块写入的接收线程可能在清理之后又插回了几行；重启时统一扫掉，
    保证库里不残留任何“这次上传”的数据。已提交任务的暂存本就为空。
    """
    open_ids = [r["id"] for r in conn.execute("SELECT id FROM upload_jobs WHERE state = 'open'")]
    for job_id in open_ids:
        conn.execute(
            "UPDATE upload_jobs SET state = 'aborted', "
            "error = COALESCE(error, '进程重启，未完成的上传已作废') WHERE id = ?",
            (job_id,),
        )
    leftovers = [
        r["id"]
        for r in conn.execute(
            "SELECT id FROM upload_jobs WHERE state != 'open' AND ("
            "  id IN (SELECT job_id FROM upload_contents)"
            "  OR id IN (SELECT job_id FROM upload_pointers)"
            "  OR id IN (SELECT job_id FROM upload_parent_blocks))"
        )
    ]
    for job_id in set(open_ids) | set(leftovers):
        _clear_staging(conn, job_id)
    conn.commit()
    return len(open_ids)
