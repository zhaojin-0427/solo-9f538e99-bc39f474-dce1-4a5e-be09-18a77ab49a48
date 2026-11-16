"""端到端自测：拉起 app + 示例源，覆盖关键链路。

用法：python3 tests/e2e_test.py
覆盖：
 1. 整份采集 -> 校验 -> 发布
 2. 断流后从已确认字节续传，不重取前缀（按源端送出字节数核对）
 3. 采集进程重启后断点与磁盘共同恢复、同版续传
 4. 源换版 -> 舍弃旧前缀整体重收，不混合两份文件
 5. 源忽略 Range 回 200 -> 舍弃旧前缀重收完整正文
 6. 损坏版本 CSV 校验失败 -> 不发布，旧观测仍在
 7. 错误 206（Content-Range 错位）-> 不推进断点
 8. 下载/中断期间旧观测仍可读取
"""
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
APP_PORT = 18080
SRC_PORT = 19000
APP = f"http://127.0.0.1:{APP_PORT}"
SRC = f"http://127.0.0.1:{SRC_PORT}"
TIDE_URL = f"{SRC}/tide.csv"

FAILED = []


def check(name, cond, detail=""):
    tag = "PASS" if cond else "FAIL"
    print(f"[{tag}] {name}" + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


def req(method, url, data=None, timeout=15):
    body = None
    headers = {}
    if data is not None:
        body = json.dumps(data).encode()
        headers["Content-Type"] = "application/json"
    r = urllib.request.Request(url, data=body, method=method, headers=headers)
    with urllib.request.urlopen(r, timeout=timeout) as resp:
        return resp.status, resp.read()


def form(url, fields, timeout=15):
    data = "&".join(f"{k}={v}" for k, v in fields.items()).encode()
    r = urllib.request.Request(url, data=data, method="POST",
                               headers={"Content-Type":
                                        "application/x-www-form-urlencoded"})
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        # Flask 表单处理后 302 跳转，urllib 会自动跟随
        return e.code


def set_source(**kw):
    fields = {"version": kw.get("version", 1),
              "drop_after": kw.get("drop_after", 0)}
    if kw.get("ignore_range"):
        fields["ignore_range"] = "on"
    if kw.get("bad_range"):
        fields["bad_range"] = "on"
    form(f"{SRC}/control", fields)


def src_stats():
    _, body = req("GET", f"{SRC}/stats")
    return json.loads(body)


def src_reset_stats():
    req("GET", f"{SRC}/stats/reset")


def jobs():
    _, body = req("GET", f"{APP}/api/jobs")
    return json.loads(body)


def job(job_id):
    return next(j for j in jobs() if j["id"] == job_id)


def wait_status(job_id, want, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = job(job_id)["status"]
        if st in want:
            return st
        time.sleep(0.2)
    raise TimeoutError(f"job {job_id} 状态未变为 {want}，当前 {job(job_id)}")


def start_job(url=TIDE_URL):
    form(f"{APP}/jobs", {"url": url})
    j = [j for j in jobs() if j["url"] == url][0]
    return j["id"]


def continue_job(job_id):
    form(f"{APP}/jobs/{job_id}/start", {})


def reset_job(job_id):
    form(f"{APP}/jobs/{job_id}/reset", {})


def observation_csv(job_id):
    _, body = req("GET", f"{APP}/jobs/{job_id}/observation.csv")
    return body.decode()


def expected_csv(version):
    sys.path.insert(0, os.path.join(ROOT, "source"))
    import source_app
    return source_app.make_csv(version)


def wait_port(port, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz"
                                        if port == APP_PORT else
                                        f"http://127.0.0.1:{port}/state",
                                        timeout=2):
                return
        except Exception:
            time.sleep(0.3)
    raise TimeoutError(f"端口 {port} 未就绪")


def spawn_app(data_dir):
    env = dict(os.environ, APP_DATA=data_dir, APP_PORT=str(APP_PORT),
               SAMPLE_SOURCE_URL=TIDE_URL)
    return subprocess.Popen(
        [sys.executable, os.path.join(ROOT, "app", "app.py")],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def main():
    data_dir = tempfile.mkdtemp(prefix="tide-e2e-")
    src_env = dict(os.environ, PORT=str(SRC_PORT))
    src = subprocess.Popen(
        [sys.executable, os.path.join(ROOT, "source", "source_app.py")],
        env=src_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    app_proc = spawn_app(data_dir)
    try:
        wait_port(SRC_PORT)
        wait_port(APP_PORT)
        set_source(version=1, drop_after=0)
        src_reset_stats()

        # ---- 1. 整份采集 -> 校验 -> 发布 ----
        jid = start_job()
        st = wait_status(jid, ("done", "failed", "interrupted"))
        j = job(jid)
        check("1.整份采集完成", st == "done", j["message"])
        check("1.发布 144 行", j["has_observation"] and j["observation_rows"] == 144)
        check("1.内容与源一致", observation_csv(jid) == expected_csv(1))
        gets = [r for r in src_stats()["requests"] if r["method"] == "GET"]
        check("1.源仅收到 1 次 GET 且送出全部字节",
              len(gets) == 1 and gets[0]["bytes_sent"] == len(expected_csv(1).encode()),
              str(gets))

        # ---- 2. 断流 -> 续传，不重取前缀 ----
        set_source(version=2, drop_after=1500)
        src_reset_stats()
        continue_job(jid)  # 预检发现 v1->v2 换版，重置后整体重收，1500 字节处断流
        st = wait_status(jid, ("interrupted", "failed"))
        j = job(jid)
        check("2.断流后任务可续传", st == "interrupted", j["message"])
        check("2.断点停在 1500", j["confirmed_bytes"] == 1500,
              str(j["confirmed_bytes"]))
        set_source(version=2, drop_after=0)
        continue_job(jid)
        st = wait_status(jid, ("done", "failed"))
        j = job(jid)
        check("2.续传后完成", st == "done", j["message"])
        check("2.内容与 v2 完全一致", observation_csv(jid) == expected_csv(2))
        gets = [r for r in src_stats()["requests"] if r["method"] == "GET"]
        ranged = [r for r in gets if r["range"]]
        check("2.续传请求带 Range 且起点=断点",
              len(ranged) == 1 and ranged[0]["range"] == "bytes=1500-",
              str(gets))
        total_sent = sum(r["bytes_sent"] for r in gets)
        check("2.累计送出字节 == 文件总长（未重取前缀）",
              total_sent == len(expected_csv(2).encode()),
              f"sent={total_sent} size={len(expected_csv(2).encode())}")

        # ---- 3. 进程重启后续传 ----
        set_source(version=1, drop_after=2000)
        src_reset_stats()
        continue_job(jid)  # 换版 v2->v1，重置，2000 字节处断流
        wait_status(jid, ("interrupted", "failed"))
        app_proc.send_signal(signal.SIGKILL)  # 模拟采集进程崩溃
        app_proc.wait()
        app_proc = spawn_app(data_dir)  # 同一数据目录重启
        wait_port(APP_PORT)
        j = job(jid)
        check("3.重启后断点与磁盘共同恢复",
              j["confirmed_bytes"] == 2000 and j["status"] == "interrupted",
              f"confirmed={j['confirmed_bytes']} status={j['status']}")
        part = os.path.join(data_dir, "parts", f"job_{jid}.part")
        check("3.磁盘保留字节 == 断点", os.path.getsize(part) == 2000,
              str(os.path.getsize(part)))
        set_source(version=1, drop_after=0)
        continue_job(jid)
        st = wait_status(jid, ("done", "failed"))
        check("3.重启后续传完成且内容正确",
              st == "done" and observation_csv(jid) == expected_csv(1))
        gets = [r for r in src_stats()["requests"] if r["method"] == "GET"]
        check("3.重启后未重取前缀",
              sum(r["bytes_sent"] for r in gets) == len(expected_csv(1).encode()),
              str(gets))

        # ---- 4. 部分下载期间源换版 -> 舍弃前缀，不混合 ----
        reset_job(jid)
        set_source(version=1, drop_after=1000)
        continue_job(jid)  # 重置后整体重收 v1，断流在 1000
        wait_status(jid, ("interrupted",))
        check("4.断流中断点", job(jid)["confirmed_bytes"] == 1000)
        set_source(version=2, drop_after=0)  # 源换版 v1->v2
        src_reset_stats()
        continue_job(jid)
        st = wait_status(jid, ("done", "failed"))
        got = observation_csv(jid)
        check("4.换版后整体重收且与 v2 完全一致（未混合）",
              st == "done" and got == expected_csv(2))
        gets = [r for r in src_stats()["requests"] if r["method"] == "GET"]
        check("4.换版后首个 GET 不带 Range（舍弃前缀）",
              gets and gets[0]["range"] is None and gets[0]["status"] == 200,
              str(gets))

        # ---- 5. 源忽略 Range 回 200 -> 舍弃前缀重收 ----
        reset_job(jid)
        set_source(version=2, drop_after=800)
        continue_job(jid)
        wait_status(jid, ("interrupted",))
        check("5.断流中断点 800", job(jid)["confirmed_bytes"] == 800)
        set_source(version=2, drop_after=0, ignore_range=True)
        src_reset_stats()
        continue_job(jid)
        st = wait_status(jid, ("done", "failed"))
        got = observation_csv(jid)
        check("5.忽略 Range 回 200 后重收完整正文",
              st == "done" and got == expected_csv(2),
              job(jid)["message"])
        gets = [r for r in src_stats()["requests"] if r["method"] == "GET"]
        check("5.源对 Range 请求回了 200 全量",
              any(r["range"] and r["status"] == 200 for r in gets), str(gets))
        check("5.未混合（长度等于 v2 全长）",
              len(got.encode()) == len(expected_csv(2).encode()))
        set_source(version=2, drop_after=0)  # 关闭 ignore_range

        # ---- 6. 损坏版本校验失败 -> 旧观测保留 ----
        set_source(version=3, drop_after=0)
        continue_job(jid)
        st = wait_status(jid, ("failed", "done"))
        j = job(jid)
        check("6.损坏 CSV 校验失败", st == "failed" and "校验失败" in j["message"],
              j["message"])
        check("6.旧观测（v2）仍在", observation_csv(jid) == expected_csv(2))

        # ---- 7. 错误 206 不推进断点 ----
        reset_job(jid)
        set_source(version=2, drop_after=600)
        continue_job(jid)
        wait_status(jid, ("interrupted",))
        check("7.断流中断点 600", job(jid)["confirmed_bytes"] == 600)
        set_source(version=2, drop_after=0, bad_range=True)
        continue_job(jid)
        st = wait_status(jid, ("failed",))
        j = job(jid)
        check("7.错误 206 被拒绝", "206" in j["message"], j["message"])
        check("7.断点未推进", j["confirmed_bytes"] == 600,
              str(j["confirmed_bytes"]))
        set_source(version=2, drop_after=0)
        continue_job(jid)
        st = wait_status(jid, ("done", "failed"))
        check("7.恢复正常后完成", st == "done"
              and observation_csv(jid) == expected_csv(2))

        # ---- 8. 下载/中断期间旧观测可读 ----
        set_source(version=1, drop_after=900)
        continue_job(jid)  # 换版 v2->v1，断流
        deadline = time.time() + 15
        while time.time() < deadline and job(jid)["confirmed_bytes"] == 0:
            time.sleep(0.1)
        check("8.部分下载期间旧观测（v2）仍可读取",
              observation_csv(jid) == expected_csv(2))
        wait_status(jid, ("interrupted",))  # 等首个会话结束，避免与续传撞锁
        set_source(version=1, drop_after=0)
        continue_job(jid)
        wait_status(jid, ("done",))
        check("8.完成后观测替换为 v1", observation_csv(jid) == expected_csv(1))

        print()
        if FAILED:
            print(f"共 {len(FAILED)} 项失败: {FAILED}")
            return 1
        print("全部端到端检查通过")
        return 0
    finally:
        for p in (app_proc, src):
            try:
                p.send_signal(signal.SIGTERM)
                p.wait(timeout=5)
            except Exception:
                p.kill()


if __name__ == "__main__":
    sys.exit(main())
