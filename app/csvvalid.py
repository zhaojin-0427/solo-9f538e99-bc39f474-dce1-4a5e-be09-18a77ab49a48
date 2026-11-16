"""潮位 CSV 校验：UTC 时刻 + 有限潮位，时刻严格递增。

格式约定：两列（时刻, 潮位），首行可为表头；时刻须为带时区的 ISO 8601
（支持 ...Z 或 +00:00 等），统一归一到 UTC 后比较。
"""
import csv
import io
import math
from datetime import datetime, timezone


def parse_utc(s):
    s = s.strip()
    if not s:
        raise ValueError("时刻为空")
    if s.endswith(("Z", "z")):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        raise ValueError(f"时刻无法解析: {s!r}")
    if dt.tzinfo is None:
        raise ValueError(f"时刻缺少时区: {s!r}")
    return dt.astimezone(timezone.utc)


def _looks_like_data(row):
    if len(row) < 2:
        return False
    try:
        parse_utc(row[0])
        float(row[1])
        return True
    except ValueError:
        return False


def validate_tide_csv(text):
    """校验 CSV 文本，返回 (rows, first_ts, last_ts)。

    rows 为 [(datetime, float), ...]。任何一项不满足即抛 ValueError。
    """
    if not isinstance(text, str) or not text.strip():
        raise ValueError("CSV 为空")
    try:
        raw_rows = list(csv.reader(io.StringIO(text)))
    except csv.Error as e:
        raise ValueError(f"CSV 解析失败: {e}")
    raw_rows = [r for r in raw_rows if r and any(c.strip() for c in r)]
    if not raw_rows:
        raise ValueError("CSV 无有效行")
    if _looks_like_data(raw_rows[0]):
        data_rows = raw_rows          # 无表头
    else:
        data_rows = raw_rows[1:]      # 首行当表头
    if not data_rows:
        raise ValueError("CSV 无数据行")

    rows = []
    prev = None
    for i, r in enumerate(data_rows, start=1):
        if len(r) < 2:
            raise ValueError(f"第{i}个数据行列数不足（需时刻与潮位两列）")
        t = parse_utc(r[0])
        try:
            level = float(r[1].strip())
        except ValueError:
            raise ValueError(f"第{i}个数据行潮位不是数值: {r[1]!r}")
        if not math.isfinite(level):
            raise ValueError(f"第{i}个数据行潮位不是有限值: {r[1]!r}")
        if prev is not None and t <= prev:
            raise ValueError(f"第{i}个数据行时刻未严格递增: {r[0]!r}")
        prev = t
        rows.append((t, level))
    return rows, rows[0][0], rows[-1][0]
