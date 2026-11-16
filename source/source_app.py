"""本地示例潮位源：强 ETag + Range，支持可切换版本 / 断流 / 忽略 Range / 错误 206。

控制面板在 GET /，可随时切换：
- version: 1 / 2 为两份不同但合法的潮位 CSV；3 为校验必失败的损坏版
- drop_after: >0 时每次响应在送出该字节数后提前断开（模拟断网）
- ignore_range: 开启后对 Range 请求一律回 200 全量（模拟不支持 Range 的源）
- bad_range: 开启后 206 的 Content-Range 起点故意错开（模拟异常源）

GET /stats 返回请求日志（方法、Range、状态、实际送出字节），供核对续传行为。
"""
import hashlib
import math
import threading
from datetime import datetime, timedelta, timezone

from flask import Flask, Response, jsonify, redirect, request, url_for

app = Flask(__name__)

_lock = threading.Lock()
STATE = {
    "version": 1,
    "drop_after": 0,      # 0 = 不断流
    "ignore_range": False,
    "bad_range": False,
}
REQUEST_LOG = []          # [{method, range, status, bytes_sent}]


def make_csv(version):
    """确定性生成单日潮位 CSV（10 分钟间隔，144 行）。"""
    day = datetime(2026, 1, 15, tzinfo=timezone.utc)
    lines = ["utc_time,level_m"]
    if version == 3:
        # 损坏版：NaN 潮位 + 时刻倒退，校验必失败
        for i in range(10):
            t = day + timedelta(minutes=10 * i)
            lines.append(f"{t.strftime('%Y-%m-%dT%H:%M:%SZ')},1.234")
        lines.append(f"{(day + timedelta(minutes=100)).strftime('%Y-%m-%dT%H:%M:%SZ')},NaN")
        lines.append(f"{(day + timedelta(minutes=50)).strftime('%Y-%m-%dT%H:%M:%SZ')},1.5")
        return "\n".join(lines) + "\n"
    amp, phase, n = {1: (1.8, 0.0, 144), 2: (2.3, 1.1, 150)}[version]
    for i in range(n):
        t = day + timedelta(minutes=10 * i)
        level = 2.0 + amp * math.sin(2 * math.pi * i / 72 + phase) \
            + 0.3 * math.sin(2 * math.pi * i / 12)
        lines.append(f"{t.strftime('%Y-%m-%dT%H:%M:%SZ')},{level:.3f}")
    return "\n".join(lines) + "\n"


_cache = {}


def current_body():
    with _lock:
        version = STATE["version"]
    if version not in _cache:
        body = make_csv(version).encode("utf-8")
        etag = '"tide-v%d-%s"' % (version, hashlib.sha256(body).hexdigest()[:12])
        _cache[version] = (body, etag)
    return _cache[version]


def _log(method, range_hdr, status, sent):
    with _lock:
        REQUEST_LOG.append({"method": method, "range": range_hdr,
                            "status": status, "bytes_sent": sent})


@app.route("/tide.csv", methods=["GET", "HEAD"])
def tide():
    body, etag = current_body()
    size = len(body)
    with _lock:
        drop_after = STATE["drop_after"]
        ignore_range = STATE["ignore_range"]
        bad_range = STATE["bad_range"]

    common = {"ETag": etag, "Accept-Ranges": "bytes",
              "Content-Type": "text/csv; charset=utf-8",
              "Cache-Control": "no-cache"}

    range_hdr = request.headers.get("Range")
    if_range = request.headers.get("If-Range")

    start, end = 0, size - 1
    status = 200
    if range_hdr and not ignore_range:
        # If-Range：版本已变则按 RFC 回 200 全量
        if if_range and if_range.strip() != etag:
            pass
        else:
            try:
                unit, rng = range_hdr.split("=", 1)
                s, e = rng.split("-", 1)
                if unit.strip() != "bytes":
                    raise ValueError
                start = int(s)
                end = int(e) if e.strip() else size - 1
                end = min(end, size - 1)
                if start >= size or start < 0:
                    _log(request.method, range_hdr, 416, 0)
                    return Response(status=416, headers={
                        **common, "Content-Range": f"bytes */{size}"})
                status = 206
            except ValueError:
                _log(request.method, range_hdr, 400, 0)
                return Response(status=400, headers=common)

    if status == 206:
        cr_start = start + 512 if bad_range else start  # 故意错位，测试客户端核对
        headers = {**common,
                   "Content-Range": f"bytes {cr_start}-{end}/{size}",
                   "Content-Length": str(end - start + 1)}
    else:
        headers = {**common, "Content-Length": str(size)}

    if request.method == "HEAD":
        _log("HEAD", range_hdr, status, 0)
        return Response(status=status, headers=headers)

    payload = body[start:end + 1]
    limit = drop_after if drop_after > 0 else None

    def gen():
        sent = 0
        try:
            for off in range(0, len(payload), 1024):
                chunk = payload[off:off + 1024]
                if limit is not None and sent + len(chunk) > limit:
                    chunk = chunk[:max(0, limit - sent)]
                if not chunk:
                    return
                sent += len(chunk)
                yield chunk
                if limit is not None and sent >= limit:
                    return  # 提前结束：Content-Length 未满足，连接被关闭
        finally:
            _log("GET", range_hdr, status, sent)

    return Response(gen(), status=status, headers=headers)


CONTROL_PAGE = """<!doctype html><html lang="zh"><head><meta charset="utf-8">
<title>示例潮位源控制台</title>
<style>body{font-family:system-ui,"Noto Sans CJK SC",sans-serif;margin:2rem}
label{display:block;margin:.5rem 0}button{padding:.3rem .8rem}
code{background:#f3f4f6;padding:0 .3rem}</style></head><body>
<h1>示例潮位源控制台</h1>
<p>数据地址：<code>/tide.csv</code>（强 ETag，支持 Range / If-Range / HEAD）</p>
<form method="post" action="/control">
<label>版本：
 <select name="version">
  <option value="1" $v1>1 — 合法，144 行</option>
  <option value="2" $v2>2 — 合法，150 行（不同潮位）</option>
  <option value="3" $v3>3 — 损坏版（NaN、时刻倒退，校验必失败）</option>
 </select></label>
<label>断流：每次响应送出 <input name="drop_after" type="number" min="0" value="$drop" style="width:7rem"> 字节后断开（0 = 正常）</label>
<label><input type="checkbox" name="ignore_range" $ir> 忽略 Range（对续传请求回 200 全量）</label>
<label><input type="checkbox" name="bad_range" $br> 错误 206（Content-Range 起点故意错位）</label>
<button type="submit">应用</button>
</form>
<p><a href="/stats">请求统计 (JSON)</a> · <a href="/stats/reset">清空统计</a></p>
</body></html>"""


@app.get("/")
def control_page():
    with _lock:
        s = dict(STATE)
    from string import Template
    return Template(CONTROL_PAGE).substitute(
        v1="selected" if s["version"] == 1 else "",
        v2="selected" if s["version"] == 2 else "",
        v3="selected" if s["version"] == 3 else "",
        drop=s["drop_after"],
        ir="checked" if s["ignore_range"] else "",
        br="checked" if s["bad_range"] else "")


@app.post("/control")
def control():
    with _lock:
        STATE["version"] = int(request.form.get("version", STATE["version"]))
        STATE["drop_after"] = max(0, int(request.form.get("drop_after") or 0))
        STATE["ignore_range"] = "ignore_range" in request.form
        STATE["bad_range"] = "bad_range" in request.form
    if request.accept_mimetypes.best == "application/json":
        return jsonify(STATE)
    return redirect(url_for("control_page"))


@app.get("/state")
def state():
    with _lock:
        return jsonify(dict(STATE))


@app.get("/stats")
def stats():
    with _lock:
        log = list(REQUEST_LOG)
    return jsonify({"requests": log,
                    "total_bytes_sent": sum(r["bytes_sent"] for r in log)})


@app.route("/stats/reset")
def stats_reset():
    with _lock:
        REQUEST_LOG.clear()
    if request.accept_mimetypes.best == "application/json":
        return jsonify({"ok": True})
    return redirect(url_for("control_page"))


if __name__ == "__main__":
    import os
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "9000")),
            threaded=True)
