#!/usr/bin/env python3
"""Throwaway test for the partial-line guard in log_to_influx.process_file.

Confirms:
  * a torn (newline-less) trailing line is NOT recorded as a truncated 6.0 ask,
  * complete lines are recorded unchanged, in order (no regression for healthy feeds),
  * the read position parks before ANY incomplete trailing line so it is re-read whole.

Exercises the real patched process_file. Run from the umm repo root:
    ./venv/bin/python3.10 test_partial_line_guard.py
"""
import os
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "umm"))  # repo root so `scripts.md_feed...` resolves
from scripts.md_feed import log_to_influx as L  # noqa: E402


def mk_line(symbol, et, ask_price, ask_qty="10.070000"):
    """A well-formed Symbol-format line; ask price is the last field on the line."""
    return (f"[2026-08-04 23:38:20.900000] INFO Symbol: {symbol} | "
            f"Exchange TS: {et} | Receive TS: {et} | Parse TS: {et} | "
            f"Bid: 66.610000 x 63.836810 | Ask: {ask_qty} x {ask_price}\n")


FULL = mk_line("PERP-CRCL-USDG", 1785886700942000000, "63.846803")
TORN = FULL.rstrip("\n")[:-8]  # drop newline + "3.846803" -> line ends "... x 6"


class FakeClient:
    def __init__(self):
        self.points = []

    def switch_database(self, db):
        pass

    def write_points(self, body):
        self.points.extend(body)


def run_process(contents):
    """Write `contents` to a temp md_feed log, run the real process_file, return (asks, saved_pos, size)."""
    d = tempfile.mkdtemp()
    path = os.path.join(d, "20260804_231426.bvod.derived_arcus_bnbfut_md_5.md_feed.log")
    with open(path, "w") as f:
        f.write(contents)
    client = FakeClient()
    L.shutdown_flag = False
    L.process_file(path, client, "UMM_MD", 1, "DERIVED_ARCUS_BNBFUT", None, tail=False)
    asks = [p["fields"]["ask_price"] for p in client.points]
    return asks, L.load_position(path), len(contents)


def check(name, cond):
    print(("PASS" if cond else "FAIL"), "-", name)
    return cond


ok = True

# --- The bug the guard contains (parse_log_line itself is unchanged) ---
ok &= check("parse_log_line(full) -> 63.846803", L.parse_log_line(FULL.strip())["ask_price"] == 63.846803)
ok &= check("parse_log_line(torn) -> 6.0 (the bug)", L.parse_log_line(TORN)["ask_price"] == 6.0)

# --- A complete line + a torn trailing line: only the complete one recorded, no 6.0 ---
asks, pos, size = run_process(FULL + TORN)
ok &= check("torn trailing line NOT recorded", asks == [63.846803])
ok &= check("no 6.0 recorded", 6.0 not in asks)
ok &= check("position parked before torn line", pos == len(FULL))

# --- REGRESSION: a normal multi-line file records every line, unchanged, in order ---
a = mk_line("PERP-CRCL-USDG", 1, "63.10")
b = mk_line("PERP-BABA-USDG", 2, "88.20")
c = mk_line("PERP-GLD-USDG", 3, "512.30")
asks, pos, size = run_process(a + b + c)
ok &= check("3 complete lines -> 3 points (nothing dropped)", len(asks) == 3)
ok &= check("values recorded unchanged and in order", asks == [63.10, 88.20, 512.30])
ok &= check("position advanced to EOF", pos == size)

# --- REGRESSION: an arbitrary newline-less trailing line parks the position before it ---
# (a generic mid-line cut, not the price-truncation case, to show it's the newline check that matters)
head = mk_line("PERP-CRCL-USDG", 4, "63.40")
incomplete = "[2026-08-04 23:38:21.000000] INFO Symbol: PERP-CRCL-USDG | Exchange TS: 178588"
asks, pos, size = run_process(head + incomplete)
ok &= check("complete line before the incomplete one IS recorded", asks == [63.40])
ok &= check("incomplete trailing line NOT recorded", len(asks) == 1)
ok &= check("position parked before the incomplete line", pos == len(head))

print("\nALL PASSED" if ok else "\nSOME FAILED")
sys.exit(0 if ok else 1)
