"""Flask 入口：恢复点浏览、还原镜像下载、清退任务、链尾上传追加恢复点。"""
from __future__ import annotations

import os
import sqlite3

from flask import Flask, Response, g, jsonify, render_template, request

from . import chain, upload
from .db import connect, init_db
from .seed import seed_if_empty


def create_app(db_path: str | None = None) -> Flask:
    db_path = db_path or os.environ.get("BACKUP_DB", "/data/backups.db")
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)

    app = Flask(__name__)
    app.config["DB_PATH"] = db_path
    # 上传镜像按块流式读取，不整盘进内存；显式放开请求体上限（长度由块数校验）
    app.config["MAX_CONTENT_LENGTH"] = None

    # 启动专用连接：建表、种样例、重启恢复（恢复点半成品任务作废）
    boot = connect(db_path)
    init_db(boot)
    seed_if_empty(boot)
    stale_count = chain.recover_pending_jobs(boot)
    stale_uploads = upload.recover_pending_uploads(boot)
    app.config["STALE_ON_START"] = stale_count
    app.config["STALE_UPLOADS_ON_START"] = stale_uploads
    boot.close()

    def db() -> sqlite3.Connection:
        # 每线程/每请求一个连接，避免 SQLite 跨线程检查；teardown 时关闭
        if "db" not in g:
            g.db = connect(app.config["DB_PATH"])
        return g.db

    @app.teardown_appcontext
    def _close_db(exc):
        conn = g.pop("db", None)
        if conn is not None:
            conn.close()

    # ------------------------------------------------------------ errors
    @app.errorhandler(chain.NotFoundError)
    def _not_found(err):
        return jsonify(error="not_found", message=str(err)), 404

    @app.errorhandler(chain.LengthMismatchError)
    def _length(err):
        return jsonify(error="length_mismatch", message=str(err)), 400

    @app.errorhandler(chain.ConflictError)
    def _conflict(err):
        return jsonify(error="conflict", message=str(err)), 409

    @app.errorhandler(chain.DomainError)
    def _domain(err):
        return jsonify(error="domain_error", message=str(err)), 400

    # ------------------------------------------------------------ pages
    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/healthz")
    def healthz():
        return jsonify(status="ok", chain_version=chain.chain_version(db()))

    # ------------------------------------------------------------ api
    @app.get("/api/chain")
    def api_chain():
        return jsonify(chain.chain_snapshot(db()))

    @app.get("/api/jobs")
    def api_jobs():
        return jsonify([dict(r) for r in chain.list_jobs(db())])

    @app.post("/api/backups/<backup_id>/prepare")
    def api_prepare(backup_id: str):
        return jsonify(chain.prepare_eviction(db(), backup_id)), 201

    @app.post("/api/jobs/<job_id>/publish")
    def api_publish(job_id: str):
        return jsonify(chain.publish_eviction(db(), job_id))

    @app.post("/api/jobs/<job_id>/abandon")
    def api_abandon(job_id: str):
        return jsonify(chain.abandon_job(db(), job_id))

    # ----------------------------------------------------------- uploads
    @app.get("/api/uploads")
    def api_uploads():
        return jsonify([dict(r) for r in upload.list_upload_jobs(db())])

    @app.get("/api/uploads/hint")
    def api_upload_hint():
        """供页面/脚本预检尺寸，避免把整盘传完才发现长度不符。"""
        conn = db()
        block_size, block_count = chain.disk_spec(conn)
        tail = chain.list_backups(conn)[-1]
        return jsonify(
            expected_size=block_size * block_count,
            block_size=block_size,
            block_count=block_count,
            tail_id=tail["id"],
            tail_seq=tail["seq"],
        )

    @app.post("/api/uploads")
    def api_upload_begin():
        # 选定链尾（页面选择或上传前命中的链尾）+ 文件元数据；返回 upload_id
        payload = request.get_json(silent=True) or {}
        parent_id = payload.get("parent_id")
        if not parent_id:
            return jsonify(error="bad_request", message="必须指定链尾恢复点"), 400
        declared = payload.get("size")
        if declared is not None:
            try:
                declared = int(declared)
            except (TypeError, ValueError):
                return jsonify(error="bad_request", message="size 必须是整数"), 400
        out = upload.begin_upload(
            db(),
            parent_id=parent_id,
            filename=str(payload.get("filename") or "upload.img"),
            label=str(payload.get("label") or ""),
            declared_size=declared,
        )
        return jsonify(out), 201

    @app.put("/api/uploads/<job_id>/data")
    @app.post("/api/uploads/<job_id>/data")
    def api_upload_data(job_id: str):
        # 请求体就是原始镜像字节；逐块读取，不做 request.data / get_data()
        try:
            out = upload.receive_and_commit(db(), job_id, request.stream)
        except chain.DomainError:
            raise
        except Exception as exc:  # 网络中断等：任务已作废，返回 400 供页面提示
            return jsonify(error="upload_failed", message=f"上传失败: {exc}"), 400
        return jsonify(out), 201

    @app.post("/api/uploads/<job_id>/abort")
    def api_upload_abort(job_id: str):
        reason = (request.get_json(silent=True) or {}).get("reason") or "客户端取消上传"
        return jsonify(upload.abort_upload(db(), job_id, str(reason)))

    @app.post("/api/uploads/recover")
    def api_upload_recover():
        # 便于运维在确认无活跃上传时手动清理（启动时已自动执行一次）
        return jsonify(recovered=upload.recover_pending_uploads(db()))

    # --------------------------------------------------------- download
    @app.get("/backups/<backup_id>/download")
    def download(backup_id: str):
        # 预读元数据（走请求连接）
        conn = db()
        target = chain.get_backup(conn, backup_id)
        block_size, block_count = chain.disk_spec(conn)
        db_path = app.config["DB_PATH"]

        def generate():
            # 流式迭代发生在请求上下文 teardown 之后，使用专用连接并自行关闭
            stream_conn = connect(db_path)
            try:
                yield from chain.materialize_blocks(stream_conn, backup_id)
            finally:
                stream_conn.close()

        filename = f"restore-P{target['seq']}.img"
        resp = Response(generate(), mimetype="application/octet-stream")
        resp.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
        resp.headers["Content-Length"] = str(block_size * block_count)
        return resp

    return app


if __name__ == "__main__":
    # `python -m app.server`（容器 CMD）走这里；测试通过 create_app(path) 注入临时库
    app = create_app()
    app.run(host="0.0.0.0", port=8000, threaded=True)
