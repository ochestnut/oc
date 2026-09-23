"""Shared local analytics paths. JST_ANALYTICS_DIR overrides the output root."""
from pathlib import Path
import os

ANALYTICS_ROOT = Path(os.environ.get("JST_ANALYTICS_DIR", str(Path.home() / "Documents/jst/analytics"))).expanduser()
OC_ROOT = Path(__file__).resolve().parents[2]
JST_ROOT = OC_ROOT.parent

def output_path(name, area="data"):
    """Resolve a generated filename and create its parent; retain explicit absolute paths."""
    path = Path(name).expanduser()
    if not path.is_absolute():
        if path.parts and path.parts[0] == "data":
            path = Path(*path.parts[1:])
        path = ANALYTICS_ROOT / area / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return path
