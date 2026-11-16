"""单元自测：CSV 校验与 Content-Range 解析。用法：python3 tests/unit_test.py"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "app"))

import csvvalid
from collector import parse_content_range, strong_etag_of

FAILED = []


def check(name, cond, detail=""):
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + ("" if cond else f" — {detail}"))
    if not cond:
        FAILED.append(name)


def expect_invalid(name, text, needle=""):
    try:
        csvvalid.validate_tide_csv(text)
        check(name, False, "本应失败却通过")
    except ValueError as e:
        check(name, needle in str(e), f"异常信息不含 {needle!r}: {e}")


# ---- CSV 校验 ----
good = "utc_time,level_m\n2026-01-15T00:00:00Z,1.5\n2026-01-15T00:10:00+00:00,-0.25\n"
rows, first, last = csvvalid.validate_tide_csv(good)
check("合法 CSV（混合 Z 与 +00:00）通过", len(rows) == 2 and rows[1][1] == -0.25)

no_header = "2026-01-15T00:00:00Z,1.5\n2026-01-15T00:10:00Z,1.6\n"
rows, _, _ = csvvalid.validate_tide_csv(no_header)
check("无表头 CSV 通过且首行不丢", len(rows) == 2)

expect_invalid("时刻倒退被拒绝",
               "t,l\n2026-01-15T00:10:00Z,1.0\n2026-01-15T00:00:00Z,1.1\n",
               "严格递增")
expect_invalid("时刻并列被拒绝",
               "t,l\n2026-01-15T00:00:00Z,1.0\n2026-01-15T00:00:00Z,1.1\n",
               "严格递增")
expect_invalid("NaN 潮位被拒绝", "t,l\n2026-01-15T00:00:00Z,NaN\n", "有限值")
expect_invalid("inf 潮位被拒绝", "t,l\n2026-01-15T00:00:00Z,inf\n", "有限值")
expect_invalid("非数值潮位被拒绝", "t,l\n2026-01-15T00:00:00Z,abc\n", "不是数值")
expect_invalid("无时区时刻被拒绝", "t,l\n2026-01-15 00:00:00,1.0\n", "时区")
expect_invalid("空 CSV 被拒绝", "", "空")
expect_invalid("只有表头被拒绝", "t,l\n", "无数据行")
expect_invalid("列数不足被拒绝", "t,l\n2026-01-15T00:00:00Z\n", "列数")

# 跨时区归一：同一物理时刻不同时区写法视为不递增
expect_invalid("等值时刻（不同时区写法）被拒绝",
               "t,l\n2026-01-15T08:00:00+08:00,1.0\n2026-01-15T00:00:00Z,1.1\n",
               "严格递增")

# ---- Content-Range 解析 ----
check("解析 bytes 100-199/1000",
      parse_content_range("bytes 100-199/1000") == (100, 199, 1000))
check("解析未知总长", parse_content_range("bytes 0-9/*") == (0, 9, None))
check("拒绝畸形 Content-Range", parse_content_range("bytes 9-0/100") is None
      and parse_content_range("items 0-9/10") is None
      and parse_content_range("") is None)

# ---- 强 ETag 判定 ----
check("强 ETag 接受", strong_etag_of({"ETag": '"abc123"'}) == '"abc123"')
check("弱 ETag 拒绝", strong_etag_of({"ETag": 'W/"abc"'}) is None)
check("缺失 ETag", strong_etag_of({}) is None)

print()
if FAILED:
    print(f"{len(FAILED)} 项失败")
    sys.exit(1)
print("全部单元检查通过")
