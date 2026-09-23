from pathlib import Path
import importlib
import analytics_paths

def test_output_root_override_and_parent_creation(tmp_path, monkeypatch):
    monkeypatch.setenv("JST_ANALYTICS_DIR", str(tmp_path / "results"))
    module = importlib.reload(analytics_paths)
    try:
        p = module.output_path("nested/report.pdf", "reports")
        assert p == tmp_path / "results/reports/nested/report.pdf"
        assert p.parent.is_dir()
        assert module.output_path("data/example.csv") == tmp_path / "results/data/example.csv"
        explicit = tmp_path / "custom/file.csv"
        assert module.output_path(explicit) == explicit
        assert explicit.parent.is_dir()
    finally:
        monkeypatch.delenv("JST_ANALYTICS_DIR")
        importlib.reload(analytics_paths)
