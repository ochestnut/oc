"""Archive audit contracts using local parquet fixtures and mocked S3 listings."""
import importlib.util
import json
from pathlib import Path
import sys

import pandas as pd
import pytest

spec = importlib.util.spec_from_file_location("check_booktop_archives", Path(__file__).parents[1] / "check_booktop_archives.py")
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
T = pd.Timestamp("2026-09-11 12:00")
WINDOW = ("HTX", "PAIR-BTC-USD", "ap-northeast-1a_TY03", T)
FILES = {"production": ["old"], "direct": ["new"]}


def frame(offsets=(0, 1), prices=(100., 101.)):
    return pd.DataFrame([dict(timestamp=T + pd.Timedelta(seconds=offset),
                              recv=T + pd.Timedelta(seconds=offset, milliseconds=2),
                              bid_price=price, ask_price=price + 1, bid_qty=2., ask_qty=3.)
                         for offset, price in zip(offsets, prices)])


def compare(a, b):
    return module.compare(WINDOW, FILES, {"old": a, "new": b}.__getitem__)


def test_order_and_numeric_dtype_do_not_change_equality():
    a = frame()
    b = a.iloc[::-1].copy()
    b["bid_qty"] = b.bid_qty.astype(int)
    assert compare(a, b)["records_equal"] is True


def test_extra_unchanged_quotes_are_visible_but_changes_match():
    result = compare(frame((0, 2), (100., 101.)), frame((0, 1, 2), (100., 100., 101.)))
    assert result["status"] == "different"
    assert result["extra_records"] == 1
    assert result["state_changes_equal"] is True


@pytest.mark.parametrize("column,value", [("bid_price", 999.), ("bid_qty", 999.), ("timestamp", T + pd.Timedelta(seconds=3))])
def test_price_size_and_timestamp_changes_fail(column, value):
    a = frame()
    b = a.copy()
    b.loc[1, column] = value
    result = compare(a, b)
    assert result["records_equal"] is False
    assert result["state_changes_equal"] is False


def test_receipt_difference_is_not_hidden_by_state_comparison():
    a = frame()
    b = a.copy()
    b["recv"] += pd.Timedelta(nanoseconds=1)
    result = compare(a, b)
    assert result["records_equal"] is False
    assert result["state_changes_equal"] is True
    assert compare(a, b.drop(columns="recv"))["records_equal"] is False


def test_duplicate_multiplicity_and_null_quantities():
    a = frame()
    a["bid_qty"] = float("nan")
    assert compare(a, a)["records_equal"] is True
    result = compare(a, pd.concat([a, a.iloc[:1]], ignore_index=True))
    assert result["extra_records"] == 1
    assert result["state_changes_equal"] is True


@pytest.mark.parametrize("bad", [pd.DataFrame(), frame().drop(columns="ask_qty"), frame((1000,), (100.,))])
def test_invalid_chunks_are_errors_not_matches(bad):
    assert compare(frame(), bad)["status"] == "error"


def key(rec, symbol="PAIR-BTC-USD", hhmm="1200"):
    return f"bucket/market_data/HTX/booktop/{symbol}/2026-09-11/HTX_{symbol}_booktop_2026-09-11_{rec}_{hhmm}.parquet"


def test_coverage_pairs_exact_recorder_and_window(tmp_path):
    prod = "ap-northeast-1a_TY03"
    keys = [key(prod), key(prod + "D"), key(prod, hhmm="1215"), key(prod + "D", symbol="PAIR-ETH-USD")]
    windows = module.inventory(keys, T, T + pd.Timedelta(minutes=30), set())
    summary, code = module.audit(windows, lambda _: frame(), tmp_path, 2)
    assert summary["production_windows"] == 2
    assert summary["missing_direct_windows"] == 1
    assert summary["object_coverage_complete"] is False
    assert summary["statuses"] == {"equal": 1, "missing_direct": 1, "direct_only": 1}
    assert code == 1
    assert len((tmp_path / "windows.jsonl").read_text().splitlines()) == 3


def test_no_baseline_cannot_pass(tmp_path):
    summary, code = module.audit({}, lambda _: frame(), tmp_path, 1)
    assert code == 2
    assert summary["object_coverage_complete"] is False


def test_duplicate_objects_are_errors():
    assert module.compare(WINDOW, {"production": ["a", "b"], "direct": ["c"]}, lambda _: frame())["status"] == "error"


def test_read_error_prevents_success_even_with_object_coverage(tmp_path):
    def broken(_):
        raise OSError("unreadable parquet")
    summary, code = module.audit({WINDOW: FILES}, broken, tmp_path, 1)
    assert summary["object_coverage_complete"] is True
    assert summary["exact_match"] is False
    assert summary["statuses"] == {"error": 1}
    assert code == 2


def test_discovery_failure_invalidates_previous_success(tmp_path, monkeypatch):
    (tmp_path / "summary.json").write_text('{"completed": true, "exact_match": true}')
    def broken(**_):
        raise OSError("listing failed")
    monkeypatch.setattr(module, "s3_client", broken)
    assert module.main(["--start", str(T), "--end", "2026-09-11 12:15",
                        "--output-dir", str(tmp_path)]) == 2
    assert json.loads((tmp_path / "summary.json").read_text())["completed"] is False


def test_materialized_cache_is_not_production_inventory():
    ordinary = key("ap-northeast-1a_TY03")
    derived = ordinary.replace("market_data/", "market_data/any/")
    assert len(module.inventory([ordinary, derived], T, T + pd.Timedelta(minutes=15), set())) == 1


def test_prediction_layout_and_scope():
    prod = "ap-northeast-1a_TY03"
    name = "PRED-BTC_260911-USD"
    path = f"bucket/market_data/X/booktop/predictions/2026-09-11/{name}/X_{name}_booktop_2026-09-11_{prod}D_1200.parquet"
    windows = module.inventory([path, key("eu-central-1a_FR01")], T, T + pd.Timedelta(minutes=15), {prod})
    assert list(windows) == [("X", name, prod, T)]


def test_s3_listing_limits_dates_and_ignores_other_data_types():
    class FS:
        prefixes = []
        def ls(self, prefix, detail=True):
            children = {"bucket/market_data": ["HTX"], "bucket/market_data/HTX/booktop": ["PAIR-BTC-USD", "predictions"]}
            return [{"name": prefix + "/" + child, "type": "directory"} for child in children.get(prefix, [])]
        def find(self, prefix):
            self.prefixes.append(prefix)
            return [prefix + "/x.parquet", prefix + "/x.txt"]
    fs = FS()
    keys = module.s3_keys(fs, "bucket", T, T + pd.Timedelta(minutes=15))
    assert len(keys) == 2
    assert all(prefix.endswith("2026-09-11") for prefix in fs.prefixes)


def test_cli_reads_real_parquet_and_writes_reports(tmp_path):
    for rec in ("ap-northeast-1a_TY03", "ap-northeast-1a_TY03D"):
        path = tmp_path / key(rec).removeprefix("bucket/")
        path.parent.mkdir(parents=True, exist_ok=True)
        frame().to_parquet(path)
    output = tmp_path / "report"
    code = module.main(["--local-root", str(tmp_path), "--start", str(T), "--end", "2026-09-11 12:15",
                        "--output-dir", str(output), "--recorder", "TY03"])
    assert code == 0
    assert json.loads((output / "summary.json").read_text())["exact_match"] is True
    # Reuse listing only; contents must still be read and compared afresh.
    changed = frame()
    changed.loc[0, "bid_price"] = 999.
    changed.to_parquet(path)
    assert module.main(["--local-root", str(tmp_path), "--start", str(T), "--end", "2026-09-11 12:15",
                        "--output-dir", str(output), "--inventory", str(output / "inventory.json")]) == 1
    assert module.main(["--local-root", str(tmp_path), "--start", str(T), "--end", "2026-09-11 12:30",
                        "--output-dir", str(output), "--inventory", str(output / "inventory.json")]) == 2
