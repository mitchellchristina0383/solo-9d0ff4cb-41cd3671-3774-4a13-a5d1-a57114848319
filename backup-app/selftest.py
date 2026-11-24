"""端到端自测（无需 Docker，直接对应用逻辑 + Flask 测试客户端打）。

覆盖：
  T1 清零语义：缺省继承、显式清零、清零后重写“复活”
  T2 清退正确性：存活恢复点逐字节不变、编号不重排、无临时整盘
  T3 交错清退：同一旧链两笔任务，先发布生效、后发布 409 并可重试
  T4 中断重启：准备后“重启”，任务作废、仍服务最后发布的链，重发可成功
  T5 HTTP 冒烟：页面、下载 Content-Length、镜像哈希与真值一致、边界 409
  T6 链尾上传：继承/清零语义、下载逐字节一致、长度不符与中断零残留
  T7 接收期间清退：比对不受影响，两种先后次序都能正确接上
  T8 两份上传争用链尾：先提交生效，后提交 409 并提示重选链尾
  T9 重启：未完成上传作废、暂存清空、链不变
"""
from __future__ import annotations

import hashlib
import io
import os
import tempfile

from app import chain, upload
from app.db import connect, init_db
from app.seed import BLOCK_COUNT, BLOCK_SIZE, block_content, expected_image
from app.server import create_app

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name} {detail}")


def image_bytes(conn, backup_id: str) -> bytes:
    return b"".join(chain.materialize_blocks(conn, backup_id))


def images_before(conn) -> dict[str, bytes]:
    snap = chain.chain_snapshot(conn)
    return {b["id"]: image_bytes(conn, b["id"]) for b in snap["backups"]}


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="backup-selftest-")
    db_path = os.path.join(tmp, "test.db")
    print(f"自测数据库: {db_path}\n")

    # -------------------------------------------------------- T1 清零语义
    print("T1 清零/继承语义（种子链真值比对）")
    conn = connect(db_path)
    init_db(conn)
    from app.seed import seed_if_empty
    seed_if_empty(conn)

    snap = chain.chain_snapshot(conn)
    check("种子链有 5 个恢复点", len(snap["backups"]) == 5)
    check("链版本初始为 0", snap["chain_version"] == 0)
    check("块大小/块数正确", (snap["block_size"], snap["block_count"]) == (64, 8))

    bs = snap["block_size"]
    for b in snap["backups"]:
        got = image_bytes(conn, b["id"])
        want = expected_image(b["seq"])
        check(f"P{b['seq']} 还原镜像与真值一致", got == want,
              f"got {hashlib.sha256(got).hexdigest()[:12]} "
              f"want {hashlib.sha256(want).hexdigest()[:12]}")

    p4 = image_bytes(conn, next(b["id"] for b in snap["backups"] if b["seq"] == 4))
    check("P4 槽3 仍为显式零（P3 清零后无人改写）", p4[3*bs:4*bs] == b"\x00"*bs)
    check("P4 槽6 为显式零（P4 自己清零）", p4[6*bs:7*bs] == b"\x00"*bs)
    check("P4 槽1 清零后被 P4 重写复活（非零）", p4[1*bs:2*bs] != b"\x00"*bs)
    check("P4 槽0 继承到 P3 的 D", p4[0*bs:1*bs][:1] == b"D")
    check("P4 槽2 一路继承 P1 的 B", p4[2*bs:3*bs][:1] == b"B")

    # -------------------------------------------------------- T2 清退正确性
    print("\nT2 中间恢复点清退（P2 → 并入 P3）")
    before = images_before(conn)
    p2 = next(b["id"] for b in snap["backups"] if b["seq"] == 2)
    prep = chain.prepare_eviction(conn, p2)
    check("准备阶段返回 ready 任务", prep["state"] == "ready" and prep["merged_blocks"] == 2,
          str(prep))
    # 准备期间原链仍可逐点还原且内容不变
    mid = images_before(conn)
    check("准备期间原链 5 个恢复点全部仍可还原", len(mid) == 5)
    check("准备期间所有还原内容与准备前完全一致", mid == before)
    check("暂存期间被清退点仍在链上",
          any(b["id"] == p2 for b in chain.chain_snapshot(conn)["backups"]))

    pub = chain.publish_eviction(conn, prep["job_id"])
    check("发布后链版本 0 → 1", pub["new_version"] == 1)
    after_snap = chain.chain_snapshot(conn)
    surviving = after_snap["backups"]
    check("恢复点数量 5 → 4", len(surviving) == 4)
    seqs = [b["seq"] for b in surviving]
    check("编号保留原 seq、出现缺口且不重排", seqs == [0, 1, 3, 4], str(seqs))
    check("UUID 保持不变", set(before) - {p2} == {b["id"] for b in surviving})
    for b in surviving:
        got = image_bytes(conn, b["id"])
        check(f"存活点 P{b['seq']} 还原内容与清退前逐字节一致", got == before[b["id"]])
    check("已清退点不可再还原（404）",
          _expect_not_found(conn, p2))
    check("重复发布同一任务报冲突",
          _expect_conflict(lambda: chain.publish_eviction(conn, prep["job_id"])))
    check("块内容仍在 blocks 表（哈希共享，不随指针删除）",
          conn.execute("SELECT COUNT(*) c FROM blocks").fetchone()["c"] >= 6)

    # 数据目录里不应出现任何整盘镜像临时文件
    leftovers = [f for f in os.listdir(tmp) if f.endswith((".img", ".tmp"))]
    check("全程未生成临时整盘文件", leftovers == [], str(leftovers))

    # 再清退一个点验证可重复操作：P1 → 并入 P3（现在 P3 是 P1 的直接后继）
    print("\nT2b 第二次清退（P1 → 并入当前后继 P3），链版本再 +1")
    p1 = next(b["id"] for b in surviving if b["seq"] == 1)
    before2 = images_before(conn)
    prep2 = chain.prepare_eviction(conn, p1)
    pub2 = chain.publish_eviction(conn, prep2["job_id"])
    check("第二次发布后链版本 1 → 2", pub2["new_version"] == 2)
    s2 = chain.chain_snapshot(conn)
    check("恢复点数量 4 → 3，编号 [0,3,4]",
          [b["seq"] for b in s2["backups"]] == [0, 3, 4])
    for b in s2["backups"]:
        check(f"存活点 P{b['seq']} 内容仍与最初一致",
              image_bytes(conn, b["id"]) == before2[b["id"]])

    # -------------------------------------------------------- T3 交错清退
    print("\nT3 交错清退（同一旧链两笔任务的乐观并发）")
    p3 = next(b["id"] for b in s2["backups"] if b["seq"] == 3)
    job_a = chain.prepare_eviction(conn, p3)
    check("已有 ready 任务时再准备被拒绝（串行化）",
          _expect_conflict(lambda: chain.prepare_eviction(conn, p3)))
    # 模拟另一笔基于同一旧链（version=2）的任务：放弃 A 后准备 B，再让 A 仍旧发布
    job_b_payload = dict(job_a)  # 同一基线版本 2、同一清退对象的等价任务
    chain.abandon_job(conn, job_a["job_id"])
    job_b = chain.prepare_eviction(conn, p3)
    check("两任务基线版本相同（同一旧链）",
          job_b["based_on_version"] == job_b_payload["based_on_version"] == 2)
    pub_b = chain.publish_eviction(conn, job_b["job_id"])
    check("先发布的 B 生效，链版本 → 3", pub_b["new_version"] == 3)
    # 手工把 A 以相同基线重新插回，制造“基于同一旧链的迟到任务”
    stale_id = _insert_ready_job(conn, p3, based_on_version=2)
    err = _expect_conflict_msg(conn, stale_id)
    check("后发布的 A 报 409 并提示重试", err is not None and "重试" in err, err or "")
    check("失败任务被标记 stale",
          chain.get_job(conn, stale_id)["state"] == "stale")
    check("基于新链（v3）重新准备并发布可以成功",
          _retry_succeeds(conn))

    # -------------------------------------------------------- T4 中断重启
    print("\nT4 中断 / 重启（最后发布的链持久化，半成品任务作废）")
    # 用独立库，避免受前面清退结果影响
    restart_db = os.path.join(tmp, "restart.db")
    conn = connect(restart_db)
    init_db(conn)
    seed_if_empty(conn)
    # 先完成一笔已发布清退，制造“最后发布的链”（版本 1）
    p2_id = next(b["id"] for b in chain.chain_snapshot(conn)["backups"] if b["seq"] == 2)
    chain.publish_eviction(conn, chain.prepare_eviction(conn, p2_id)["job_id"])
    current = chain.chain_snapshot(conn)
    before3 = images_before(conn)
    p4_id = next(b["id"] for b in current["backups"] if b["seq"] == 4)
    p1_id = next(b["id"] for b in current["backups"] if b["seq"] == 1)
    job_c = chain.prepare_eviction(conn, p1_id)
    check("中断前暂存区已写入",
          conn.execute("SELECT COUNT(*) c FROM staged_blocks WHERE job_id=?",
                       (job_c["job_id"],)).fetchone()["c"] == job_c["merged_blocks"])

    # 模拟进程重启：重新连接、重新跑启动恢复
    conn2 = connect(restart_db)
    init_db(conn2)
    stale_n = chain.recover_pending_jobs(conn2)
    check("重启后 ready 任务被标记 stale", stale_n >= 1 and
          chain.get_job(conn2, job_c["job_id"])["state"] == "stale")
    restarted = chain.chain_snapshot(conn2)
    check("重启后服务的是最后发布的链（版本 1，编号 [0,1,3,4]）",
          restarted["chain_version"] == 1 and
          [b["seq"] for b in restarted["backups"]] == [0, 1, 3, 4])
    for b in restarted["backups"]:
        check(f"重启后 P{b['seq']} 还原内容不变",
              image_bytes(conn2, b["id"]) == before3[b["id"]])
    check("重启后不能直接发布旧任务",
          _expect_conflict(lambda: chain.publish_eviction(conn2, job_c["job_id"])))
    retry = chain.prepare_eviction(conn2, p1_id)
    again = chain.publish_eviction(conn2, retry["job_id"])
    check("重新发起清退可成功，链版本 → 2", again["new_version"] == 2)
    check("最终编号 [0,3,4]",
          [b["seq"] for b in chain.chain_snapshot(conn2)["backups"]] == [0, 3, 4])
    check("末端 P4 内容自始至终未变",
          image_bytes(conn2, p4_id) == before3[p4_id])
    conn2.close()
    conn.close()

    # -------------------------------------------------------- T5 HTTP 冒烟
    print("\nT5 HTTP 层冒烟（Flask 测试客户端 + 流式下载）")
    http_db = os.path.join(tmp, "http.db")
    app = create_app(http_db)
    client = app.test_client()
    r = client.get("/")
    check("GET / 返回页面", r.status_code == 200 and "备份链".encode() in r.data)
    r = client.get("/healthz")
    check("GET /healthz", r.status_code == 200 and r.get_json()["status"] == "ok")
    r = client.get("/api/chain")
    chain_json = r.get_json()
    check("GET /api/chain 返回 5 个恢复点", len(chain_json["backups"]) == 5)
    target = next(b for b in chain_json["backups"] if b["seq"] == 4)
    r = client.get(f"/backups/{target['id']}/download")
    check("下载状态 200 且 Content-Length=512",
          r.status_code == 200 and r.headers["Content-Length"] == "512")
    check("下载镜像哈希与真值一致",
          hashlib.sha256(r.data).digest() == hashlib.sha256(expected_image(4)).digest())
    check("下载文件名带编号", "P4" in r.headers["Content-Disposition"])
    r = client.get("/backups/nonexistent/download")
    check("不存在恢复点下载返回 404", r.status_code == 404)

    full = next(b for b in chain_json["backups"] if b["kind"] == "full")
    last = chain_json["backups"][-1]
    check("清退全备返回 409",
          client.post(f"/api/backups/{full['id']}/prepare").status_code == 409)
    check("清退末端恢复点返回 409",
          client.post(f"/api/backups/{last['id']}/prepare").status_code == 409)
    mid_id = chain_json["backups"][2]["id"]
    r = client.post(f"/api/backups/{mid_id}/prepare")
    check("合法清退准备返回 201", r.status_code == 201)
    jid = r.get_json()["job_id"]
    r = client.post(f"/api/jobs/{jid}/publish")
    check("HTTP 发布成功并版本 +1",
          r.status_code == 200 and r.get_json()["new_version"] == 1)

    # 应用工厂重启路径：再 create_app 一次，ready 任务应被自动清理
    app2 = create_app(http_db)
    with app2.test_client() as c2:
        check("工厂重启后无 ready 任务残留",
              all(j["state"] != "ready" for j in c2.get("/api/jobs").get_json()))
        check("工厂重启后链版本保持为 1",
              c2.get("/api/chain").get_json()["chain_version"] == 1)

    conn.close()

    # --------------------------------------------------- T6 链尾上传追加
    up_db = os.path.join(tmp, "upload.db")
    conn = connect(up_db)
    init_db(conn)
    seed_if_empty(conn)
    print("\nT6 从链尾上传同尺寸镜像，追加恢复点")
    base = chain.chain_snapshot(conn)
    tail = base["backups"][-1]
    p4_img = image_bytes(conn, tail["id"])
    new_img = bytearray(p4_img)
    new_img[2*BLOCK_SIZE:3*BLOCK_SIZE] = block_content("X", 2)   # 改写
    new_img[5*BLOCK_SIZE:6*BLOCK_SIZE] = b"\x00" * BLOCK_SIZE     # P4 原为 C，改为显式清零
    new_img = bytes(new_img)

    blocks_before = conn.execute("SELECT COUNT(*) c FROM blocks").fetchone()["c"]
    job = upload.begin_upload(conn, tail["id"], "vm.raw", "T6 上传", len(new_img))
    check("上传前登记返回 open 与预期尺寸",
          job["state"] == "open" and job["expected_size"] == len(p4_img), str(job))
    check("登记不改变链",
          [b["id"] for b in chain.chain_snapshot(conn)["backups"]]
          == [b["id"] for b in base["backups"]])
    res = upload.receive_and_commit(conn, job["upload_id"], io.BytesIO(new_img))
    check("追加成功、类型增量、seq 接在链尾", res["state"] == "committed"
          and res["kind"] == "inc" and res["seq"] == tail["seq"] + 1, str(res))
    check("与所选点相同块沿用继承（6 块）", res["inherited"] == 6, str(res))
    check("改写块记新写（1 块）", res["written"] == 1, str(res))
    check("全零块记显式清零（1 块）", res["zeroed"] == 1, str(res))
    new_id = res["backup_id"]
    ptrs = {r["block_index"]: r["is_zero"] for r in chain.list_pointers(conn, new_id)}
    check("新点只显式记录变化槽位（2 与 5）", sorted(ptrs) == [2, 5], str(ptrs))
    check("清零槽位以 is_zero=1 标记", ptrs[5] == 1)
    check("继承槽位不写指针", 0 not in ptrs and 7 not in ptrs)
    restored = image_bytes(conn, new_id)
    check("还原内容与上传逐字节一致", restored == new_img,
          f"{hashlib.sha256(restored).hexdigest()[:12]} vs "
          f"{hashlib.sha256(new_img).hexdigest()[:12]}")
    check("整体镜像 SHA-256 记录一致",
          hashlib.sha256(new_img).hexdigest() == res["image_sha256"])
    check("链版本 +1", chain.chain_snapshot(conn)["chain_version"] == 1)

    # 断电/中断后的暂存表必须为空
    check("提交后暂存内容清空",
          conn.execute("SELECT COUNT(*) c FROM upload_contents").fetchone()["c"] == 0)
    check("提交后暂存指针清空",
          conn.execute("SELECT COUNT(*) c FROM upload_pointers").fetchone()["c"] == 0)
    check("提交后父点快照清空",
          conn.execute("SELECT COUNT(*) c FROM upload_parent_blocks").fetchone()["c"] == 0)

    # 再传一次“完全相同”的镜像：应全部继承、零新块
    blocks_mid = conn.execute("SELECT COUNT(*) c FROM blocks").fetchone()["c"]
    job2 = upload.begin_upload(conn, new_id, "vm-same.raw", "T6 原样再传", len(new_img))
    res2 = upload.receive_and_commit(conn, job2["upload_id"], io.BytesIO(new_img))
    check("原样再传：8 块全部沿用继承", res2["inherited"] == BLOCK_COUNT, str(res2))
    check("原样再传：不新增块内容",
          conn.execute("SELECT COUNT(*) c FROM blocks").fetchone()["c"] == blocks_mid)
    check("原样再传：新点无显式指针",
          chain.list_pointers(conn, res2["backup_id"]) == [])
    check("原样再传：还原仍逐字节一致",
          image_bytes(conn, res2["backup_id"]) == new_img)

    # 长度不符：少一个字节，应在开始前就拒绝（不建任务）
    tail_now = chain.chain_snapshot(conn)["backups"][-1]
    n_jobs_before = conn.execute("SELECT COUNT(*) c FROM upload_jobs").fetchone()["c"]
    err = _expect_short(lambda: upload.begin_upload(
        conn, tail_now["id"], "short.raw", "短", len(new_img) - 1))
    check("尺寸不符在登记时被拒（LengthMismatchError）", err)
    check("尺寸不符不留下上传任务",
          conn.execute("SELECT COUNT(*) c FROM upload_jobs").fetchone()["c"] == n_jobs_before)

    # 长度不符：声明正确但实际流少字节 → 接收中断
    blocks_before2 = conn.execute("SELECT COUNT(*) c FROM blocks").fetchone()["c"]
    imgs_before2 = images_before(conn)
    job3 = upload.begin_upload(conn, tail_now["id"], "short2.raw", "短流", len(new_img))
    err = _expect_short(lambda: upload.receive_and_commit(
        conn, job3["upload_id"], io.BytesIO(new_img[:-1])))
    check("流长度不足被识别为长度不符", err)
    check("中断后任务标记 aborted",
          upload.get_upload_job(conn, job3["upload_id"])["state"] == "aborted")
    check("中断后无新块进入 blocks",
          conn.execute("SELECT COUNT(*) c FROM blocks").fetchone()["c"] == blocks_before2)
    check("中断后无新恢复点",
          [b["id"] for b in chain.chain_snapshot(conn)["backups"]]
          == list(imgs_before2))
    check("中断后暂存表清空",
          conn.execute("SELECT COUNT(*) c FROM upload_contents").fetchone()["c"] == 0
          and conn.execute("SELECT COUNT(*) c FROM upload_pointers").fetchone()["c"] == 0)

    # 长度超限：多一个字节
    job4 = upload.begin_upload(conn, tail_now["id"], "long.raw", "长流", len(new_img))
    check("流长度超出被拒绝",
          _expect_short(lambda: upload.receive_and_commit(
              conn, job4["upload_id"], io.BytesIO(new_img + b"\x00"))))
    check("超限后无新恢复点",
          [b["id"] for b in chain.chain_snapshot(conn)["backups"]] == list(imgs_before2))
    check("超限后无新块",
          conn.execute("SELECT COUNT(*) c FROM blocks").fetchone()["c"] == blocks_before2)

    # 非链尾作为父点：拒绝
    first_id = chain.chain_snapshot(conn)["backups"][0]["id"]
    check("选非链尾上传被 409 拒绝",
          _expect_conflict(lambda: upload.begin_upload(
              conn, first_id, "x.raw", "x", len(new_img))))

    # ------------------------------------------------ T7 接收期间清退
    print("\nT7 接收与比对期间中间点照常清退")
    # 场景 A：清退先发布，上传仍在接收其余块（父点仍是链尾，有效内容不变）
    conn.close()
    conn = connect(up_db)
    snap = chain.chain_snapshot(conn)
    evict_id = snap["backups"][1]["id"]     # 中间点
    parent_id = snap["backups"][-1]["id"]
    parent_img = image_bytes(conn, parent_id)
    sample = bytearray(parent_img)
    sample[0:BLOCK_SIZE] = block_content("Z", 0)
    sample = bytes(sample)

    job = upload.begin_upload(conn, parent_id, "vm2.raw", "T7 并发清退", len(sample))
    # 只让前两块可读，模拟“接收尚未完成”；上传线程用自己的连接
    stream = _GatedStream(
        [sample[i*BLOCK_SIZE:(i+1)*BLOCK_SIZE] for i in range(BLOCK_COUNT)],
        gate_after=2,
    )
    import threading
    box = {}

    def run_upload():
        tconn = connect(up_db)
        try:
            box["result"] = upload.receive_and_commit(tconn, job["upload_id"], stream)
        except Exception as e:  # pragma: no cover
            box["error"] = e
        finally:
            tconn.close()

    t = threading.Thread(target=run_upload)
    t.start()
    stream.wait_gate()
    # 此刻上传挂在第 3 块上；清退一笔中间点并发布
    ev = chain.prepare_eviction(conn, evict_id)
    pub = chain.publish_eviction(conn, ev["job_id"])
    check("上传接收中发布清退成功（写锁未被上传长占）",
          pub["state"] == "published", str(pub))
    check("清退未改变父点（链尾）有效内容",
          image_bytes(conn, parent_id) == parent_img)
    stream.release()
    t.join(timeout=10)
    check("接收线程正常结束", "error" not in box, str(box.get("error")))
    res_a = box.get("result", {})
    check("清退期间完成的上传仍接在原父点之后",
          res_a.get("state") == "committed" and res_a.get("parent_id") == parent_id,
          str(res_a))
    check("新点还原与上传逐字节一致",
          image_bytes(conn, res_a.get("backup_id", "")) == sample)

    # 场景 B：上传先提交，随后清退中间点；新点内容不受影响
    before_b = image_bytes(conn, res_a["backup_id"])
    snap_b = chain.chain_snapshot(conn)
    mid_b = snap_b["backups"][1]["id"]
    ev_b = chain.prepare_eviction(conn, mid_b)
    chain.publish_eviction(conn, ev_b["job_id"])
    check("清退后上传所得新点仍逐字节一致",
          image_bytes(conn, res_a["backup_id"]) == before_b)

    # 场景 C：接收期间清退的正是父点的直接前驱 —— 父点作为后继被并入指针，
    # backup_blocks 行会变，但有效内容不变，比对与提交仍应成功。
    # 用独立库，保证存在“前驱有父点未覆盖的槽位”的确定性布局（P3 → P4）。
    c_db = os.path.join(tmp, "t7c.db")
    c_conn = connect(c_db)
    init_db(c_conn)
    seed_if_empty(c_conn)
    snap_c0 = chain.chain_snapshot(c_conn)
    parent_c0 = snap_c0["backups"][-1]       # P4
    pred_c0 = snap_c0["backups"][-2]         # P3，有父点未覆盖的 0/3 槽
    parent_img_c0 = image_bytes(c_conn, parent_c0["id"])
    sample_c = bytearray(parent_img_c0)
    sample_c[7*BLOCK_SIZE:8*BLOCK_SIZE] = block_content("W", 7)
    sample_c = bytes(sample_c)
    job_c = upload.begin_upload(c_conn, parent_c0["id"], "vm3.raw", "T7 清退前驱",
                                len(sample_c))
    stream_c = _GatedStream(
        [sample_c[i*BLOCK_SIZE:(i+1)*BLOCK_SIZE] for i in range(BLOCK_COUNT)],
        gate_after=3,
    )
    box_c = {}

    def run_upload_c():
        tconn = connect(c_db)
        try:
            box_c["result"] = upload.receive_and_commit(tconn, job_c["upload_id"], stream_c)
        except Exception as e:  # pragma: no cover
            box_c["error"] = e
        finally:
            tconn.close()

    tc = threading.Thread(target=run_upload_c)
    tc.start()
    stream_c.wait_gate()
    rows_before = len(chain.list_pointers(c_conn, parent_c0["id"]))
    ev_c = chain.prepare_eviction(c_conn, pred_c0["id"])
    pub_c = chain.publish_eviction(c_conn, ev_c["job_id"])
    rows_after = len(chain.list_pointers(c_conn, parent_c0["id"]))
    check("清退父点前驱成功、父点指针行被并入",
          pub_c["state"] == "published" and pub_c["merged_blocks"] > 0
          and rows_after > rows_before,
          f"before={rows_before} after={rows_after} pub={pub_c}")
    check("父点有效内容在指针并入后仍不变",
          image_bytes(c_conn, parent_c0["id"]) == parent_img_c0)
    stream_c.release()
    tc.join(timeout=10)
    check("父点指针被并入后上传仍完成", "error" not in box_c
          and box_c.get("result", {}).get("state") == "committed", str(box_c))
    check("场景 C 新点还原逐字节一致",
          image_bytes(c_conn, box_c["result"]["backup_id"]) == sample_c)
    c_conn.close()

    # --------------------------------------- T8 两份上传争用同一链尾
    print("\nT8 两份上传争用链尾（先提交生效，后提交提示重选）")
    snap_c = chain.chain_snapshot(conn)
    parent_c = snap_c["backups"][-1]
    parent_c_img = image_bytes(conn, parent_c["id"])
    img_a = bytearray(parent_c_img); img_a[0:BLOCK_SIZE] = block_content("A", 0)
    img_b = bytearray(parent_c_img); img_b[1*BLOCK_SIZE:2*BLOCK_SIZE] = block_content("B", 1)
    img_a, img_b = bytes(img_a), bytes(img_b)

    ja = upload.begin_upload(conn, parent_c["id"], "a.raw", "争用 A", len(img_a))
    jb = upload.begin_upload(conn, parent_c["id"], "b.raw", "争用 B", len(img_b))
    check("两份上传可同时登记在链尾", ja["upload_id"] != jb["upload_id"])

    ra = upload.receive_and_commit(conn, ja["upload_id"], io.BytesIO(img_a))
    check("先提交的 A 生效", ra["state"] == "committed", str(ra))
    version_after_a = chain.chain_snapshot(conn)["chain_version"]
    tail_after_a = chain.chain_snapshot(conn)["backups"][-1]["id"]

    blocks_pre_b = conn.execute("SELECT COUNT(*) c FROM blocks").fetchone()["c"]
    ids_pre_b = [b["seq"] for b in chain.chain_snapshot(conn)["backups"]]
    err = _expect_conflict(lambda: upload.receive_and_commit(
        conn, jb["upload_id"], io.BytesIO(img_b)))
    check("后提交的 B 报冲突", err)
    check("B 的任务标记 aborted 且说明链尾已变",
          upload.get_upload_job(conn, jb["upload_id"])["state"] == "aborted"
          and "链尾" in (upload.get_upload_job(conn, jb["upload_id"])["error"] or ""))
    check("B 不产生恢复点",
          [b["seq"] for b in chain.chain_snapshot(conn)["backups"]] == ids_pre_b)
    check("B 不产生新块",
          conn.execute("SELECT COUNT(*) c FROM blocks").fetchone()["c"] == blocks_pre_b)
    check("B 的暂存区已清空",
          conn.execute("SELECT COUNT(*) c FROM upload_pointers WHERE job_id=?",
                       (jb["upload_id"],)).fetchone()["c"] == 0)
    check("A 的新点仍是链尾", tail_after_a == ra["backup_id"])
    check("B 冲突时链版本仅因 A +1",
          chain.chain_snapshot(conn)["chain_version"] == version_after_a)
    check("A 内容不受 B 失败影响",
          image_bytes(conn, ra["backup_id"]) == img_a)

    # 管理员重选链尾后重传 B 应成功
    new_tail = chain.chain_snapshot(conn)["backups"][-1]
    retry_img = bytearray(image_bytes(conn, new_tail["id"]))
    retry_img[1*BLOCK_SIZE:2*BLOCK_SIZE] = block_content("B", 1)
    jb2 = upload.begin_upload(conn, new_tail["id"], "b.raw", "争用 B 重传",
                              len(retry_img))
    rb = upload.receive_and_commit(conn, jb2["upload_id"], io.BytesIO(bytes(retry_img)))
    check("重选链尾后 B 重传成功", rb["state"] == "committed"
          and rb["parent_id"] == new_tail["id"], str(rb))
    check("重传结果逐字节一致",
          image_bytes(conn, rb["backup_id"]) == bytes(retry_img))

    # ------------------------------------------------ T9 重启清理上传
    print("\nT9 重启：未完成上传作废、暂存清空")
    snap_d = chain.chain_snapshot(conn)
    parent_d = snap_d["backups"][-1]
    dangling = upload.begin_upload(conn, parent_d["id"], "dangling.raw", "未完成",
                                   len(parent_c_img))
    conn2 = connect(up_db)
    n = upload.recover_pending_uploads(conn2)
    check("重启后未完成上传被作废", n >= 1)
    check("作废任务状态为 aborted",
          upload.get_upload_job(conn2, dangling["upload_id"])["state"] == "aborted")
    check("重启后暂存内容为空",
          conn2.execute("SELECT COUNT(*) c FROM upload_contents").fetchone()["c"] == 0)
    check("重启后暂存指针为空",
          conn2.execute("SELECT COUNT(*) c FROM upload_pointers").fetchone()["c"] == 0)
    check("重启后链未改变",
          [b["id"] for b in chain.chain_snapshot(conn2)["backups"]]
          == [b["id"] for b in snap_d["backups"]])
    conn2.close()

    # -------------------------------------- T10 HTTP 层：上传 + 下载核对
    print("\nT10 HTTP 层：从备份页上传并下载核对")
    h_db = os.path.join(tmp, "http-upload.db")
    app = create_app(h_db)
    client = app.test_client()
    hint = client.get("/api/uploads/hint").get_json()
    check("hint 返回预期尺寸与链尾",
          hint["expected_size"] == 512 and hint["block_count"] == 8, str(hint))
    hc = chain.chain_snapshot(connect(h_db))
    tail_h = hc["backups"][-1]
    tail_conn = connect(h_db)
    tail_h_img = image_bytes(tail_conn, tail_h["id"])
    tail_conn.close()
    up = bytearray(tail_h_img)
    up[5*BLOCK_SIZE:6*BLOCK_SIZE] = b"\x00" * BLOCK_SIZE   # P4 槽5 原为 C，改为显式清零
    up[3*BLOCK_SIZE:4*BLOCK_SIZE] = block_content("Q", 3)  # 原为零，改为写入（复活）
    up = bytes(up)
    r = client.post("/api/uploads", json={
        "parent_id": tail_h["id"], "size": len(up), "filename": "vm.raw",
        "label": "HTTP 上传",
    })
    check("POST /api/uploads 返回 201", r.status_code == 201, r.get_data(as_text=True))
    upload_id = r.get_json()["upload_id"]
    r = client.put(f"/api/uploads/{upload_id}/data", data=up,
                   content_type="application/octet-stream")
    body = r.get_json()
    check("PUT 上传数据返回 201 且入链", r.status_code == 201
          and body.get("state") == "committed", r.get_data(as_text=True))
    new_backup = body["backup_id"]
    r = client.get(f"/backups/{new_backup}/download")
    check("新点可下载且 Content-Length 正确",
          r.status_code == 200 and r.headers["Content-Length"] == str(len(up)))
    check("下载内容与上传逐字节一致", r.data == up,
          f"{hashlib.sha256(r.data).hexdigest()[:12]} vs "
          f"{hashlib.sha256(up).hexdigest()[:12]}")
    zero_region = r.data[5*BLOCK_SIZE:6*BLOCK_SIZE]
    check("显式清零槽位下载为全零", zero_region == b"\x00" * BLOCK_SIZE)
    c_db = connect(h_db)
    check("清零槽位在链上记 is_zero", any(
        p["block_index"] == 5 and p["is_zero"]
        for p in chain.list_pointers(c_db, new_backup)))
    c_db.close()
    check("复活槽位下载为写入内容", r.data[3*BLOCK_SIZE:4*BLOCK_SIZE] == block_content("Q", 3))

    # HTTP：尺寸不符（在登记时）
    r = client.post("/api/uploads", json={
        "parent_id": new_backup, "size": 511, "filename": "bad.raw"})
    check("HTTP 尺寸不符返回 400", r.status_code == 400
          and r.get_json()["error"] == "length_mismatch", r.get_data(as_text=True))
    # HTTP：非链尾
    full_id = hc["backups"][0]["id"]
    r = client.post("/api/uploads", json={"parent_id": full_id, "size": 512})
    check("HTTP 选非链尾返回 409", r.status_code == 409)
    # HTTP：上传流截断
    r = client.post("/api/uploads", json={
        "parent_id": new_backup, "size": 512, "filename": "short.raw",
        "label": "HTTP 短"})
    short_id = r.get_json()["upload_id"]
    pre_seqs = [b["seq"] for b in client.get("/api/chain").get_json()["backups"]]
    pre_blocks = len(chain.list_pointers(connect(h_db), new_backup))
    r = client.put(f"/api/uploads/{short_id}/data", data=up[:300],
                   content_type="application/octet-stream")
    check("HTTP 流长度不足返回 400", r.status_code == 400, r.get_data(as_text=True))
    post_seqs = [b["seq"] for b in client.get("/api/chain").get_json()["backups"]]
    check("HTTP 中断后无恢复点残留", pre_seqs == post_seqs)
    c_db = connect(h_db)
    check("HTTP 中断后无暂存残留",
          c_db.execute("SELECT COUNT(*) c FROM upload_contents").fetchone()["c"] == 0)
    check("HTTP 中断后链尾仍是先前新点",
          chain.chain_snapshot(c_db)["backups"][-1]["id"] == new_backup)
    c_db.close()

    # 页面包含上传控件
    r = client.get("/")
    check("备份页含上传控件与链尾选择",
          "upload-file".encode() in r.data and "upload-parent".encode() in r.data)

    conn.close()
    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


class _GatedStream:
    """可控流：读到第 gate_after 块后阻塞，直到 release()。

    用于模拟“服务端正逐块接收、比对尚未完成”时并发发生清退。
    每次 read(n) 返回一个块（n 恒为块大小）。
    """

    def __init__(self, chunks, gate_after):
        import threading
        self._chunks = list(chunks)
        self._gate_after = gate_after
        self._pos = 0
        self._stopped = threading.Event()
        self._release = threading.Event()
        self._lock = threading.Lock()

    def read(self, n=-1):
        with self._lock:
            if self._pos >= self._gate_after:
                self._stopped.set()
                self._release.wait(10)
            if self._pos >= len(self._chunks):
                return b""
            chunk = self._chunks[self._pos]
            self._pos += 1
            return chunk

    def wait_gate(self):
        self._stopped.wait(10)

    def release(self):
        self._release.set()


def _expect_short(fn) -> bool:
    try:
        fn()
        return False
    except chain.LengthMismatchError:
        return True


def _expect_not_found(conn, backup_id) -> bool:
    try:
        chain.get_backup(conn, backup_id)
        return False
    except chain.NotFoundError:
        return True


def _expect_conflict(fn) -> bool:
    try:
        fn()
        return False
    except chain.ConflictError:
        return True


def _expect_conflict_msg(conn, job_id):
    try:
        chain.publish_eviction(conn, job_id)
        return None
    except chain.ConflictError as e:
        return str(e)


def _insert_ready_job(conn, evict_id, based_on_version) -> str:
    from uuid import uuid4
    from app.chain import _now
    job_id = uuid4().hex
    conn.execute(
        "INSERT INTO merge_jobs(id, evict_id, based_on_version, state, created_at) "
        "VALUES(?, ?, ?, 'ready', ?)",
        (job_id, evict_id, based_on_version, _now()),
    )
    conn.commit()
    return job_id


def _retry_succeeds(conn) -> bool:
    snap = chain.chain_snapshot(conn)
    if len(snap["backups"]) < 3:
        return True
    target = snap["backups"][1]["id"]
    try:
        job = chain.prepare_eviction(conn, target)
        out = chain.publish_eviction(conn, job["job_id"])
        return out["state"] == "published"
    except chain.ConflictError:
        return False


if __name__ == "__main__":
    raise SystemExit(main())
