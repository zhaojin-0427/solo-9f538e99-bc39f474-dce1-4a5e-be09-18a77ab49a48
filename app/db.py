"""SQLite 存储层：采集任务断点 + 已发布观测。

设计要点：
- jobs 表保存断点状态（etag / total_size / confirmed_bytes），与磁盘上的
  .part 文件共同构成可恢复状态；两者在 collector.recover_all() 中一起核对。
- observations 表只保存“整份到齐且通过 CSV 校验”的数据，发布是单条
  INSERT OR REPLACE 事务，读者永远看到上一份完整数据。
"""
import os
import sqlite3
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    url             TEXT NOT NULL UNIQUE,
    etag            TEXT,                    -- 当前正在接收版本的强 ETag
    total_size      INTEGER NOT NULL DEFAULT -1,  -- 期望总字节数，-1 未知
    confirmed_bytes INTEGER NOT NULL DEFAULT 0,   -- 已确认落盘字节数
    status          TEXT NOT NULL DEFAULT 'idle', -- idle/running/interrupted/failed/done
    message         TEXT NOT NULL DEFAULT '',
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS observations (
    url          TEXT PRIMARY KEY,
    etag         TEXT NOT NULL,
    content      TEXT NOT NULL,              -- 完整且通过校验的 CSV 正文
    row_count    INTEGER NOT NULL,
    first_ts     TEXT NOT NULL,
    last_ts      TEXT NOT NULL,
    published_at TEXT NOT NULL
);
"""

# 每次操作独立连接（WAL 模式下读写不互斥），避免跨线程共享连接。


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Store:
    def __init__(self, db_path):
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        conn = self._conn()
        conn.executescript(SCHEMA)
        conn.commit()

    def _conn(self):
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")  # 提交即落盘，断点不丢
        return conn

    # ---------- jobs ----------
    def get_or_create_job(self, url):
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM jobs WHERE url=?", (url,)).fetchone()
            if row is None:
                now = _now()
                cur = conn.execute(
                    "INSERT INTO jobs (url, created_at, updated_at) VALUES (?,?,?)",
                    (url, now, now),
                )
                conn.commit()
                row = conn.execute("SELECT * FROM jobs WHERE id=?",
                                   (cur.lastrowid,)).fetchone()
            return dict(row)
        finally:
            conn.close()

    def get_job(self, job_id):
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def list_jobs(self):
        conn = self._conn()
        try:
            rows = conn.execute("SELECT * FROM jobs ORDER BY id").fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def update_checkpoint(self, job_id, confirmed_bytes, total_size=None, etag=None):
        """推进断点。调用方必须先保证对应字节已 fsync 落盘。"""
        conn = self._conn()
        try:
            sets = ["confirmed_bytes=?", "updated_at=?"]
            args = [confirmed_bytes, _now()]
            if total_size is not None:
                sets.append("total_size=?")
                args.append(total_size)
            if etag is not None:
                sets.append("etag=?")
                args.append(etag)
            args.append(job_id)
            conn.execute(f"UPDATE jobs SET {', '.join(sets)} WHERE id=?", args)
            conn.commit()
        finally:
            conn.close()

    def reset_checkpoint(self, job_id):
        """舍弃旧前缀：断点清零、清空版本信息（换版或需整体重收时调用）。"""
        conn = self._conn()
        try:
            conn.execute(
                "UPDATE jobs SET etag=NULL, total_size=-1, confirmed_bytes=0,"
                " updated_at=? WHERE id=?",
                (_now(), job_id),
            )
            conn.commit()
        finally:
            conn.close()

    def set_status(self, job_id, status, message=""):
        conn = self._conn()
        try:
            conn.execute(
                "UPDATE jobs SET status=?, message=?, updated_at=? WHERE id=?",
                (status, message, _now(), job_id),
            )
            conn.commit()
        finally:
            conn.close()

    # ---------- observations ----------
    def publish_observation(self, url, etag, content, row_count, first_ts, last_ts):
        """替换可用观测。单事务完成，读者要么看到旧的、要么看到新的。"""
        conn = self._conn()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO observations"
                " (url, etag, content, row_count, first_ts, last_ts, published_at)"
                " VALUES (?,?,?,?,?,?,?)",
                (url, etag, content, row_count, first_ts, last_ts, _now()),
            )
            conn.commit()
        finally:
            conn.close()

    def get_observation(self, url):
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT * FROM observations WHERE url=?", (url,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
