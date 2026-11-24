"""SQLite 连接与建表。

库内同时保存：
  * 定长块的唯一内容（blocks）
  * 恢复点（backups）与恢复点到块的指针（backup_blocks）
  * 清退合并任务及其暂存区（merge_jobs / staged_blocks）

块内容只存一份，多个恢复点通过指针共享；清退中间恢复点时只改指针，
不复制块内容，也不会在盘上生成临时整盘镜像。
"""
from __future__ import annotations

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS backups (
    id          TEXT PRIMARY KEY,           -- UUID，终身不变
    seq         INTEGER NOT NULL UNIQUE,    -- 显示编号，清退后出现缺口，不重排
    kind        TEXT NOT NULL CHECK (kind IN ('full', 'inc')),
    label       TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS blocks (
    hash       TEXT PRIMARY KEY,            -- 内容哈希，去重用
    size       INTEGER NOT NULL,
    is_zero    INTEGER NOT NULL,            -- 1 表示“显式清零标记块”
    content    BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS backup_blocks (
    backup_id   TEXT NOT NULL REFERENCES backups(id),
    block_index INTEGER NOT NULL,
    block_hash  TEXT NOT NULL REFERENCES blocks(hash),
    is_zero     INTEGER NOT NULL,           -- 该恢复点在此槽位是否显式清零
    PRIMARY KEY (backup_id, block_index)
);

CREATE TABLE IF NOT EXISTS merge_jobs (
    id              TEXT PRIMARY KEY,       -- UUID
    evict_id        TEXT NOT NULL,          -- 待清退的中间恢复点
    based_on_version INTEGER NOT NULL,      -- 准备时依据的链版本（乐观并发）
    state           TEXT NOT NULL CHECK (state IN ('ready', 'published', 'stale')),
    created_at      TEXT NOT NULL,
    published_at    TEXT
);

-- 发布合并用的暂存区：只记录指针层改动，原链在发布前完全不动
CREATE TABLE IF NOT EXISTS staged_blocks (
    job_id      TEXT NOT NULL REFERENCES merge_jobs(id),
    block_index INTEGER NOT NULL,
    block_hash  TEXT NOT NULL REFERENCES blocks(hash),
    is_zero     INTEGER NOT NULL,
    PRIMARY KEY (job_id, block_index)
);

-- 上传追加恢复点的任务。上传期间只写下面几张暂存表，blocks/backups 不动；
-- 只有完整收齐、且所选恢复点仍是链尾时，才在一个事务里转正。
CREATE TABLE IF NOT EXISTS upload_jobs (
    id                 TEXT PRIMARY KEY,     -- UUID
    parent_id          TEXT NOT NULL,        -- 管理员选定的链尾
    parent_seq         INTEGER NOT NULL,
    parent_fingerprint TEXT NOT NULL,        -- 接收开始时父点逐槽指针的指纹
    based_on_version   INTEGER NOT NULL,     -- 开始上传时的链版本（仅记录）
    expected_size      INTEGER NOT NULL,     -- 块大小 × 块数
    filename           TEXT NOT NULL,
    label              TEXT NOT NULL,
    state              TEXT NOT NULL CHECK (state IN ('open', 'committed', 'aborted')),
    created_at         TEXT NOT NULL,
    committed_at       TEXT,
    error              TEXT,                 -- 失败/中断原因（state=aborted 时）
    result_backup_id   TEXT,                 -- 成功后追加的恢复点
    image_sha256       TEXT,                 -- 上传镜像整体哈希
    received_bytes     INTEGER NOT NULL DEFAULT 0
);

-- 上传中新增的块内容暂存区（按本任务去重）。提交时才并入 blocks 表，
-- 因此失败/中断不会在 blocks 里留下任何新块。
CREATE TABLE IF NOT EXISTS upload_contents (
    job_id  TEXT NOT NULL REFERENCES upload_jobs(id),
    hash    TEXT NOT NULL,
    size    INTEGER NOT NULL,
    content BLOB NOT NULL,
    PRIMARY KEY (job_id, hash)
);

-- 上传中新恢复点的块指针暂存区：与父点相同的槽位不写，显式清零写 is_zero=1。
CREATE TABLE IF NOT EXISTS upload_pointers (
    job_id      TEXT NOT NULL REFERENCES upload_jobs(id),
    block_index INTEGER NOT NULL,
    block_hash  TEXT NOT NULL,
    is_zero     INTEGER NOT NULL,
    PRIMARY KEY (job_id, block_index)
);

-- 接收开始时父点“有效内容”的逐槽快照。逐块比对只查这一张表，
-- 不把整盘或整张父点指针表放进内存；清退只改指针不改有效内容，
-- 所以接收期间父点被并发清退也不影响比对。
CREATE TABLE IF NOT EXISTS upload_parent_blocks (
    job_id      TEXT NOT NULL REFERENCES upload_jobs(id),
    block_index INTEGER NOT NULL,
    block_hash  TEXT NOT NULL,
    is_zero     INTEGER NOT NULL,
    PRIMARY KEY (job_id, block_index)
);

CREATE INDEX IF NOT EXISTS idx_bb_lookup
    ON backup_blocks (backup_id, block_index);

CREATE INDEX IF NOT EXISTS idx_upload_pointers_job
    ON upload_pointers (job_id, block_index);

CREATE INDEX IF NOT EXISTS idx_upload_contents_job
    ON upload_contents (job_id);
"""


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    # 外键打开；WAL 让“读原链”和“写暂存区”互不阻塞
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()
