"""采集器：可断点续传的 HTTP 下载状态机。

不变式（任何时刻、包括进程崩溃后都成立）：
1. .part 文件只追加、不随机写；确认字节数 == 文件前缀长度。
2. 每写一块：write -> fsync -> 提交 SQLite 断点，断点与磁盘字节共同推进。
3. 启动恢复时取 confirmed = min(磁盘字节, 断点)，文件截到 confirmed，
   断点同步改回 confirmed —— 二者一起恢复，不跳字节。
4. 同版（ETag 一致）才续传；源换版或续传返回 200 -> 舍弃旧前缀整体重收，
   绝不混合两份文件。
5. 206 必须核对 Content-Range 起点 == 断点、总长与已知一致；任何异常响应
   都不写盘、不推进断点。
6. 整份到齐且通过 CSV 校验才发布；否则已发布观测保持不变。
"""
import os
import re
import threading

import requests

import csvvalid
from db import Store

CHUNK = 64 * 1024
MAX_RESETS_PER_SESSION = 5  # 防止源版本抖动导致无限重置
TIMEOUT = (5, 30)           # (连接, 读取) 秒


class Interrupted(Exception):
    """可续传的中断：断网、对端提前关闭、进程重启等。"""


class Failed(Exception):
    """本次会话的异常终止：响应不合协议、CSV 校验失败等。断点保持不动。"""


def strong_etag_of(headers):
    et = headers.get("ETag")
    if et:
        et = et.strip()
        if et and not et.lower().startswith("w/"):
            return et
    return None


_CR_RE = re.compile(r"bytes\s+(\d+)-(\d+)/(\d+|\*)")


def parse_content_range(value):
    """解析 'bytes 100-199/1000' -> (start, end, total|None)；不符返回 None。"""
    if not value:
        return None
    m = _CR_RE.fullmatch(value.strip())
    if not m:
        return None
    start, end = int(m.group(1)), int(m.group(2))
    total = None if m.group(3) == "*" else int(m.group(3))
    if end < start:
        return None
    return start, end, total


class Collector:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.parts_dir = os.path.join(data_dir, "parts")
        os.makedirs(self.parts_dir, exist_ok=True)
        self.store = Store(os.path.join(data_dir, "collector.db"))
        self._locks = {}
        self._locks_guard = threading.Lock()

    # ---------- 路径 ----------
    def part_path(self, job_id):
        return os.path.join(self.parts_dir, f"job_{job_id}.part")

    # ---------- 启动恢复：断点与实际保留字节共同恢复 ----------
    def recover_all(self):
        for job in self.store.list_jobs():
            if job["status"] == "running":
                self.store.set_status(job["id"], "interrupted",
                                      "采集进程重启，任务可继续")
            path = self.part_path(job["id"])
            db_confirmed = job["confirmed_bytes"]
            disk = os.path.getsize(path) if os.path.exists(path) else 0
            keep = min(disk, db_confirmed)
            if disk > keep:
                with open(path, "r+b") as f:
                    f.truncate(keep)  # 截掉未被断点确认的字节
            if db_confirmed != keep:
                self.store.update_checkpoint(job["id"], keep)
            if keep == 0 and db_confirmed > 0 and not os.path.exists(path):
                # 字节丢失而断点还在：不能跳字节，整体清零重收
                self.store.reset_checkpoint(job["id"])

    # ---------- 对外入口 ----------
    def start_async(self, job_id):
        with self._locks_guard:
            lock = self._locks.setdefault(job_id, threading.Lock())
        if not lock.acquire(blocking=False):
            return False
        prev = self.store.get_job(job_id)
        prev_status = prev["status"] if prev else "idle"
        # 在返回前置 running，调用方随后轮询不会读到上一轮的终止态
        self.store.set_status(job_id, "running", "")
        t = threading.Thread(target=self._run_guarded,
                             args=(job_id, lock, prev_status), daemon=True)
        t.start()
        return True

    def _run_guarded(self, job_id, lock, prev_status):
        try:
            self.run_session(job_id, prev_status)
        finally:
            lock.release()

    # ---------- 会话主循环 ----------
    def run_session(self, job_id, prev_status="idle"):
        # 已完成/已失败的任务被再次启动：先 HEAD 预检源版本。
        # 换版 -> 舍弃旧前缀整体重收；未变 -> done 直接返回，failed 重走状态机。
        if prev_status in ("done", "failed"):
            job = self.store.get_job(job_id)
            if job and job["etag"] and self._version_changed(job):
                self._reset(job)
            elif prev_status == "done":
                self.store.set_status(job_id, "done", "已是最新版本，无需重采")
                return
        resets = 0
        try:
            while True:
                job = self.store.get_job(job_id)
                if job is None:
                    return
                path = self.part_path(job_id)
                have_partial = (job["confirmed_bytes"] > 0 and job["etag"]
                                and os.path.exists(path))

                # 整份到齐 -> 校验并发布（total=0 的空文件会走到校验并失败）
                if (job["total_size"] >= 0
                        and job["confirmed_bytes"] == job["total_size"]
                        and (job["confirmed_bytes"] > 0 or job["etag"])
                        and os.path.exists(path)):
                    self._finalize(job)
                    return

                before = job["confirmed_bytes"]
                resets_before = resets
                if have_partial:
                    # 换版预检（HEAD，尽力而为）：版本变了直接舍弃旧前缀
                    if self._version_changed(job):
                        self._reset(job)
                        resets += 1
                    else:
                        resets += self._resume_request(job)
                else:
                    if job["confirmed_bytes"] > 0 or job["etag"]:
                        # 断点与文件不一致（如 .part 丢失）：不能跳字节，清零
                        self._reset(job)
                        resets += 1
                    else:
                        self._fresh_request(job)
                if resets > MAX_RESETS_PER_SESSION:
                    raise Failed("版本反复变化，重置次数过多")
                # 防死循环：一轮请求既没推进断点也没触发重置 -> 中断待续
                now = self.store.get_job(job_id)
                if (resets == resets_before and now["confirmed_bytes"] == before
                        and now["status"] == "running"):
                    raise Interrupted("本轮请求未收到新字节，稍后继续")
        except Interrupted as e:
            self.store.set_status(job_id, "interrupted", str(e))
        except Failed as e:
            self.store.set_status(job_id, "failed", str(e))
        except requests.RequestException as e:
            self.store.set_status(job_id, "interrupted", f"网络错误: {e}")
        except Exception as e:  # 兜底：磁盘错误等，避免状态卡在 running
            self.store.set_status(job_id, "failed", f"内部错误: {e!r}")

    # ---------- 状态机各分支 ----------
    def reset_job(self, job_id):
        """公开入口：舍弃本地前缀与断点（已发布观测不受影响）。"""
        job = self.store.get_job(job_id)
        if job:
            self._reset(job)

    def _reset(self, job):
        """舍弃旧前缀：删文件、清断点。任何换版/异常重收都走这里。"""
        path = self.part_path(job["id"])
        if os.path.exists(path):
            os.remove(path)
        self.store.reset_checkpoint(job["id"])

    def _version_changed(self, job):
        try:
            r = requests.head(job["url"], timeout=TIMEOUT, allow_redirects=True)
        except requests.RequestException:
            return False  # HEAD 不通不阻塞续传，交给 GET 路径处理
        if r.status_code >= 400:
            return False
        et = strong_etag_of(r.headers)
        return bool(et) and et != job["etag"]

    def _fresh_request(self, job):
        resp = requests.get(job["url"], stream=True, timeout=TIMEOUT,
                            headers={"Accept-Encoding": "identity"})
        try:
            if resp.status_code != 200:
                raise Failed(f"源返回异常状态码 {resp.status_code}（期望 200）")
            et = strong_etag_of(resp.headers)
            if not et:
                raise Failed("源未提供强 ETag，无法安全续传")
            total = _content_length(resp.headers)
            self._begin_body(job, et, total)
            self._stream_to_disk(resp, job, expected=total)
            # 无 Content-Length 的完整 200 正文：干净读完即视为整份
            job2 = self.store.get_job(job["id"])
            if job2["total_size"] < 0:
                self.store.update_checkpoint(job["id"],
                                             job2["confirmed_bytes"],
                                             total_size=job2["confirmed_bytes"])
        finally:
            resp.close()

    def _resume_request(self, job):
        """续传。返回发生的重置次数（0 或 1）。"""
        offset = job["confirmed_bytes"]
        headers = {"Range": f"bytes={offset}-", "If-Range": job["etag"],
                   "Accept-Encoding": "identity"}
        resp = requests.get(job["url"], headers=headers, stream=True,
                            timeout=TIMEOUT)
        try:
            if resp.status_code == 200:
                # 源忽略 Range（或 If-Range 不匹配按 RFC 回 200）：
                # 正文是完整文件，舍弃旧前缀从头写，不混合。
                et = strong_etag_of(resp.headers)
                if not et:
                    raise Failed("源未提供强 ETag，无法安全续传")
                self._reset(job)
                job = self.store.get_job(job["id"])
                total = _content_length(resp.headers)
                self._begin_body(job, et, total)
                self._stream_to_disk(resp, job, expected=total)
                return 1
            if resp.status_code == 206:
                et = strong_etag_of(resp.headers)
                if et != job["etag"]:
                    # 206 却是另一个版本：不能拼到旧前缀上，舍弃重收
                    self._reset(job)
                    return 1
                cr = parse_content_range(resp.headers.get("Content-Range"))
                if cr is None:
                    raise Failed("206 缺少合法 Content-Range，不推进断点")
                start, end, total = cr
                if start != offset:
                    raise Failed(
                        f"206 起点 {start} 与断点 {offset} 不符，不推进断点")
                if total is None:
                    raise Failed("206 未给出文件总长，不推进断点")
                if job["total_size"] >= 0 and total != job["total_size"]:
                    raise Failed(
                        f"206 总长 {total} 与已知 {job['total_size']} 不一致")
                cl = _content_length(resp.headers)
                if cl >= 0 and cl != end - start + 1:
                    raise Failed("206 的 Content-Length 与 Content-Range 不符")
                if job["total_size"] < 0:
                    self.store.update_checkpoint(job["id"], offset,
                                                 total_size=total)
                    job = self.store.get_job(job["id"])
                self._stream_to_disk(resp, job, expected=end - start + 1)
                return 0
            if resp.status_code == 416:
                # 断点超出当前版本长度（源可能换版缩水）：舍弃重收
                self._reset(job)
                return 1
            raise Failed(f"续传返回异常状态码 {resp.status_code}，不推进断点")
        finally:
            resp.close()

    def _begin_body(self, job, etag, total):
        """开始接收一份（新）正文：文件截空、断点归零、记录版本与总长。"""
        path = self.part_path(job["id"])
        with open(path, "wb") as f:
            f.flush()
            os.fsync(f.fileno())
        self.store.update_checkpoint(job["id"], 0, total_size=total, etag=etag)

    def _stream_to_disk(self, resp, job, expected=-1):
        """把响应体追加到 .part，每块 write+fsync 后才提交断点。

        expected 为本响应声明的字节数（200 取 Content-Length，206 取
        Content-Range 区间长）。关闭 urllib3 的 enforce_content_length，
        自己核对实得字节：短读说明连接提前断开，已写字节安全保留，
        抛 Interrupted 等待续传。
        """
        resp.raw.enforce_content_length = False
        path = self.part_path(job["id"])
        confirmed = self.store.get_job(job["id"])["confirmed_bytes"]
        received = 0
        with open(path, "ab") as f:
            for chunk in resp.iter_content(CHUNK):
                if not chunk:
                    continue
                f.write(chunk)
                f.flush()
                os.fsync(f.fileno())
                confirmed += len(chunk)
                received += len(chunk)
                self.store.update_checkpoint(job["id"], confirmed)
        if expected >= 0 and received < expected:
            raise Interrupted(
                f"连接提前断开：本响应应得 {expected} 字节，实得 {received}")

    def _finalize(self, job):
        path = self.part_path(job["id"])
        size = os.path.getsize(path)
        if size != job["confirmed_bytes"]:
            # 磁盘与断点不一致：先共同恢复，下次会话再完成
            self.recover_all()
            raise Interrupted("断点与磁盘字节不一致，已重新对齐")
        with open(path, "rb") as f:
            raw = f.read()
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as e:
            raise Failed(f"CSV 不是合法 UTF-8: {e}")
        try:
            rows, first_ts, last_ts = csvvalid.validate_tide_csv(text)
        except ValueError as e:
            raise Failed(f"CSV 校验失败: {e}")
        self.store.publish_observation(
            job["url"], job["etag"], text, len(rows),
            first_ts.isoformat(), last_ts.isoformat())
        self.store.set_status(job["id"], "done",
                              f"已发布 {len(rows)} 行（ETag {job['etag']}）")


def _content_length(headers):
    try:
        return int(headers.get("Content-Length", "-1"))
    except ValueError:
        return -1
