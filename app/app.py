"""潮位补采网页：填写源 URL，启动/继续采集，查看已发布观测。"""
import os
import re

from flask import (Flask, Response, abort, flash, jsonify, redirect,
                   render_template, request, url_for)

from collector import Collector

DATA_DIR = os.environ.get("APP_DATA", os.path.join(os.path.dirname(__file__), "data"))
SAMPLE_URL = os.environ.get("SAMPLE_SOURCE_URL", "")

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "tide-collector-demo")

collector = Collector(DATA_DIR)
collector.recover_all()  # 启动即共同恢复断点与磁盘字节

URL_RE = re.compile(r"^https?://\S+$")


@app.get("/")
def index():
    jobs = collector.store.list_jobs()
    obs = {j["url"]: collector.store.get_observation(j["url"]) for j in jobs}
    any_running = any(j["status"] == "running" for j in jobs)
    return render_template("index.html", jobs=jobs, obs=obs,
                           any_running=any_running, sample_url=SAMPLE_URL)


@app.post("/jobs")
def create_and_start():
    url = (request.form.get("url") or "").strip()
    if not URL_RE.match(url):
        flash("请输入合法的 http(s) 源 URL", "error")
        return redirect(url_for("index"))
    job = collector.store.get_or_create_job(url)
    if collector.start_async(job["id"]):
        flash(f"已启动采集任务 #{job['id']}", "ok")
    else:
        flash(f"任务 #{job['id']} 正在运行中", "error")
    return redirect(url_for("index"))


@app.post("/jobs/<int:job_id>/start")
def start(job_id):
    if collector.store.get_job(job_id) is None:
        abort(404)
    if collector.start_async(job_id):
        flash(f"已启动/继续任务 #{job_id}", "ok")
    else:
        flash(f"任务 #{job_id} 正在运行中", "error")
    return redirect(url_for("index"))


@app.post("/jobs/<int:job_id>/reset")
def reset(job_id):
    job = collector.store.get_job(job_id)
    if job is None:
        abort(404)
    if job["status"] == "running":
        flash("任务运行中，不能重置", "error")
        return redirect(url_for("index"))
    collector.reset_job(job_id)  # 舍弃本地前缀与断点；已发布观测不受影响
    collector.store.set_status(job_id, "idle", "已手动重置断点")
    flash(f"任务 #{job_id} 断点已重置", "ok")
    return redirect(url_for("index"))


@app.get("/jobs/<int:job_id>/observation")
def observation(job_id):
    job = collector.store.get_job(job_id)
    if job is None:
        abort(404)
    obs = collector.store.get_observation(job["url"])
    if obs is None:
        flash("该源尚无已发布的完整观测", "error")
        return redirect(url_for("index"))
    rows = [line.split(",", 1) for line in obs["content"].strip().splitlines()]
    return render_template("observation.html", job=job, obs=obs, rows=rows)


@app.get("/jobs/<int:job_id>/observation.csv")
def observation_csv(job_id):
    job = collector.store.get_job(job_id)
    if job is None:
        abort(404)
    obs = collector.store.get_observation(job["url"])
    if obs is None:
        abort(404)
    return Response(obs["content"], mimetype="text/csv",
                    headers={"Content-Disposition":
                             f"attachment; filename=tide_job{job_id}.csv"})


@app.get("/api/jobs")
def api_jobs():
    out = []
    for j in collector.store.list_jobs():
        obs = collector.store.get_observation(j["url"])
        d = dict(j)
        d["has_observation"] = obs is not None
        d["observation_etag"] = obs["etag"] if obs else None
        d["observation_rows"] = obs["row_count"] if obs else None
        out.append(d)
    return jsonify(out)


@app.get("/healthz")
def healthz():
    return "ok"


if __name__ == "__main__":
    port = int(os.environ.get("APP_PORT", "8080"))
    app.run(host="0.0.0.0", port=port, threaded=True)
