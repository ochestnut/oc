#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JST_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
exec "$JST_DIR/umm/venv/bin/python" "$SCRIPT_DIR/bitstamp_market_markouts.py" "$@"
