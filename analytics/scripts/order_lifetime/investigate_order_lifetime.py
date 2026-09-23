#!/usr/bin/env python3
"""Read-only investigation of the InfluxDB 1.x ``order_lifetime`` measurement.

By default this inventories every book/host from the UMM location registry and
the analytics routing overrides. Use ``--book`` only to limit a debugging run:

    python analytics/scripts/order_lifetime/investigate_order_lifetime.py
    python analytics/scripts/order_lifetime/investigate_order_lifetime.py --book JDMO_COINBS

Credentials follow the repository convention: ``JST_INFLUX_USER`` and
``JST_INFLUX_PASSWORD`` override ``$JST_ROOT/.creds`` and ``~/.creds``. The
script only permits SHOW and SELECT statements.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Optional

import hjson
import pandas as pd
from influxdb import InfluxDBClient

from umm.analytics.trading_data._common import BOOKS, book_spec
from umm.analytics.trading_data.routing import _load_book_hosts, get_book_host

MEASUREMENT = "order_lifetime"
READ_ONLY_STATEMENTS = ("SHOW ", "SELECT ")
CANONICAL_ARCHIVE_COLUMNS = {
    "time",
    "client_id",
    "order_id",
    "price",
    "qty",
    "order_status",
    "ref_mid",
    "symbol",
    "level",
    "side",
    "original_status",
}


@dataclass(frozen=True)
class Settings:
    books: tuple[str, ...]
    host_override: Optional[str]
    port: int
    username: Optional[str]
    password: Optional[str]
    database_pattern: Optional[str]
    database: Optional[str]
    lookback: str
    sample_limit: int
    top_values: int
    verify_ssl: bool


def parse_args(argv: Optional[list[str]] = None) -> Settings:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--book",
        action="append",
        dest="books",
        default=[],
        help="Limit discovery to this book; repeat for multiple books (default: all)",
    )
    parser.add_argument(
        "--host",
        help="Explicit host override; only valid when one or more --book values are supplied",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("JST_INFLUX_PORT", "8086")),
    )
    parser.add_argument("--username", help="Override repository Influx credentials")
    parser.add_argument("--password", help="Override repository Influx credentials")
    parser.add_argument(
        "--database-pattern",
        help="Regex limiting databases; default matches books assigned to each host",
    )
    parser.add_argument(
        "--database",
        help="Analyze only this database; discovery still verifies the measurement exists",
    )
    parser.add_argument(
        "--lookback",
        default="30d",
        help="Influx duration used for sampling, such as 24h, 7d, or 4w (default: 30d)",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=5_000,
        help="Maximum recent rows fetched per database (default: 5000)",
    )
    parser.add_argument(
        "--top-values",
        type=int,
        default=15,
        help="Number of common values/status paths to display (default: 15)",
    )
    parser.add_argument(
        "--no-verify-ssl",
        action="store_true",
        help="Disable TLS certificate verification",
    )
    args = parser.parse_args(argv)

    books = tuple(dict.fromkeys(book.upper() for book in args.books))
    if args.host and not books:
        parser.error("--host requires at least one --book")

    if args.database_pattern:
        try:
            re.compile(args.database_pattern)
        except re.error as exc:
            parser.error(f"invalid --database-pattern: {exc}")
    if not re.fullmatch(r"[1-9][0-9]*(?:u|µ|ms|s|m|h|d|w)", args.lookback):
        parser.error("--lookback must be a positive Influx duration such as 24h, 7d, or 4w")
    if args.sample_limit < 1:
        parser.error("--sample-limit must be positive")
    if args.top_values < 1:
        parser.error("--top-values must be positive")

    username, password = load_influx_credentials(args.username, args.password)
    return Settings(
        books=books,
        host_override=args.host,
        port=args.port,
        username=username,
        password=password,
        database_pattern=args.database_pattern,
        database=args.database,
        lookback=args.lookback,
        sample_limit=args.sample_limit,
        top_values=args.top_values,
        verify_ssl=not args.no_verify_ssl,
    )


def credentials_path() -> str:
    """Match analytics config: $JST_ROOT/.creds, then ~/.creds."""
    jst_root = os.getenv("JST_ROOT")
    if jst_root:
        candidate = os.path.join(jst_root, ".creds")
        if os.path.isfile(candidate):
            return candidate
    return os.path.expanduser("~/.creds")


def load_influx_credentials(
    username_override: Optional[str], password_override: Optional[str]
) -> tuple[Optional[str], Optional[str]]:
    creds: dict[str, Any] = {}
    path = credentials_path()
    if os.path.isfile(path):
        with open(path, encoding="utf-8") as handle:
            creds = hjson.loads(handle.read())
    influx_creds = creds.get("INFLUX", {})
    username = (
        username_override
        or os.getenv("JST_INFLUX_USER")
        or influx_creds.get("username")
    )
    password = (
        password_override
        or os.getenv("JST_INFLUX_PASSWORD")
        or influx_creds.get("password")
    )
    return username, password


def resolve_book_host(book: str) -> str:
    """Use analytics routing, with its conventional DNS fallback on registry failure."""
    override = os.getenv("JST_INFLUX_HOST")
    if override:
        return override
    try:
        return get_book_host(book)
    except RuntimeError as exc:
        fallback = f"{book.lower().replace('_', '-')}.jstcapdev.com"
        print(
            f"Warning: book-location lookup failed ({exc}); trying {fallback}",
            file=sys.stderr,
        )
        return fallback


def make_client(settings: Settings, host: str) -> InfluxDBClient:
    return InfluxDBClient(
        host=host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        ssl=False,
        verify_ssl=settings.verify_ssl,
        timeout=30,
    )


def query_points(
    client: InfluxDBClient, statement: str, database: Optional[str] = None
) -> list[dict[str, Any]]:
    """Execute an allow-listed read-only InfluxQL statement."""
    normalized = statement.lstrip().upper()
    if not normalized.startswith(READ_ONLY_STATEMENTS):
        raise ValueError(f"Refusing non-read-only InfluxQL: {statement}")
    return list(client.query(statement, database=database).get_points())


def quote_identifier(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def heading(title: str) -> None:
    print(f"\n{'=' * 80}\n{title}\n{'=' * 80}")


def render_table(frame: pd.DataFrame, max_rows: int = 100) -> None:
    if frame.empty:
        print("(none)")
        return
    with pd.option_context(
        "display.max_rows",
        max_rows,
        "display.max_columns",
        None,
        "display.width",
        220,
        "display.max_colwidth",
        80,
    ):
        print(frame.to_string(index=False))


def discover_databases(
    client: InfluxDBClient, settings: Settings, database_pattern: str
) -> tuple[list[str], pd.DataFrame]:
    all_databases = [
        row["name"]
        for row in query_points(client, "SHOW DATABASES")
        if row["name"] != "_internal"
    ]
    if settings.database and settings.database not in all_databases:
        raise RuntimeError(f"Database {settings.database!r} does not exist")

    candidates = [settings.database] if settings.database else [
        database
        for database in all_databases
        if re.search(database_pattern, database)
    ]
    rows = []
    selected = []
    for database in candidates:
        measurements = {
            row["name"]
            for row in query_points(client, "SHOW MEASUREMENTS", database)
        }
        found = MEASUREMENT in measurements
        rows.append(
            {
                "database": database,
                "has_order_lifetime": found,
                "measurement_count": len(measurements),
            }
        )
        if found:
            selected.append(database)
    return selected, pd.DataFrame(rows)


def inspect_schema(
    client: InfluxDBClient, databases: Iterable[str]
) -> pd.DataFrame:
    measurement = quote_identifier(MEASUREMENT)
    rows = []
    for database in databases:
        for item in query_points(
            client, f"SHOW FIELD KEYS FROM {measurement}", database
        ):
            rows.append(
                {
                    "database": database,
                    "key": item["fieldKey"],
                    "kind": "field",
                    "type": item["fieldType"],
                }
            )
        for item in query_points(
            client, f"SHOW TAG KEYS FROM {measurement}", database
        ):
            rows.append(
                {
                    "database": database,
                    "key": item["tagKey"],
                    "kind": "tag",
                    "type": "string",
                }
            )
    return pd.DataFrame(rows)


def sample_rows(
    client: InfluxDBClient, database: str, settings: Settings
) -> pd.DataFrame:
    measurement = quote_identifier(MEASUREMENT)
    statement = (
        f"SELECT * FROM {measurement} "
        f"WHERE time >= now() - {settings.lookback} "
        f"ORDER BY time DESC LIMIT {settings.sample_limit}"
    )
    frame = pd.DataFrame(query_points(client, statement, database))
    if "time" in frame:
        frame["time"] = pd.to_datetime(frame["time"], utc=True, errors="coerce")
        frame = frame.sort_values("time").reset_index(drop=True)
    return frame


def representative_values(series: pd.Series, limit: int = 5) -> str:
    values = series.dropna().astype(str).drop_duplicates().head(limit).tolist()
    return ", ".join(values)


def profile_sample(frame: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "column": column,
                "sample_dtype": str(frame[column].dtype),
                "non_null": int(frame[column].notna().sum()),
                "null_pct": round(float(frame[column].isna().mean() * 100), 2),
                "distinct": int(frame[column].nunique(dropna=True)),
                "examples": representative_values(frame[column]),
            }
            for column in frame.columns
        ]
    ).sort_values(["null_pct", "distinct"], ascending=[False, True])


def first_present(columns: Iterable[str], choices: Iterable[str]) -> Optional[str]:
    available = set(columns)
    return next((choice for choice in choices if choice in available), None)


def inspect_lifecycles(frame: pd.DataFrame, top_values: int) -> None:
    order_key = first_present(
        frame.columns, ("order_id", "client_id", "client_order_id")
    )
    status_key = first_present(
        frame.columns, ("order_status", "status", "original_status")
    )
    status_columns = [
        column
        for column in ("order_status", "original_status", "status")
        if column in frame
    ]
    for column in status_columns:
        print(f"\nCommon values for {column}:")
        counts = (
            frame[column]
            .fillna("<NULL>")
            .astype(str)
            .value_counts()
            .head(top_values)
            .rename_axis(column)
            .reset_index(name="rows")
        )
        render_table(counts)

    if not order_key or not status_key:
        print("Could not infer both an order identifier and a status column.")
        return

    events = frame.dropna(subset=[order_key]).sort_values([order_key, "time"])
    paths: Counter[str] = Counter()
    summaries = []
    for order_id, group in events.groupby(order_key, sort=False):
        statuses = group[status_key].dropna().astype(str).tolist()
        compact_statuses = [
            status
            for index, status in enumerate(statuses)
            if index == 0 or status != statuses[index - 1]
        ]
        path = " -> ".join(compact_statuses) or "<NO STATUS>"
        paths[path] += 1
        first_seen = group["time"].min()
        last_seen = group["time"].max()
        summaries.append(
            {
                "order_id": order_id,
                "first_seen": first_seen,
                "last_seen": last_seen,
                "observed_lifetime_ms": (last_seen - first_seen).total_seconds() * 1_000,
                "events": len(group),
                "statuses": path,
            }
        )

    print(f"\nGrouping lifecycle events by {order_key!r} and status by {status_key!r}.")
    path_frame = pd.DataFrame(
        [{"status_path": path, "orders": count} for path, count in paths.most_common(top_values)]
    )
    print("\nCommon status paths:")
    render_table(path_frame)
    print("\nOrders with the most observed events/status changes:")
    summary_frame = pd.DataFrame(summaries).sort_values(
        ["events", "observed_lifetime_ms"], ascending=False
    )
    render_table(summary_frame.head(top_values))

    duplicate_columns = ["time", order_key, status_key]
    duplicate_count = int(frame.duplicated(duplicate_columns, keep=False).sum())
    print(f"\nRows duplicated on {duplicate_columns}: {duplicate_count:,}")


def analyze_database(
    client: InfluxDBClient, database: str, settings: Settings
) -> None:
    heading(f"Sample analysis: {database}.{MEASUREMENT}")
    frame = sample_rows(client, database, settings)
    if frame.empty:
        print(f"No rows in the last {settings.lookback}; try a larger --lookback.")
        return
    print(
        f"Fetched {len(frame):,} rows from {frame['time'].min()} through "
        f"{frame['time'].max()} (bounded sample, not a total row count)."
    )
    render_table(profile_sample(frame))

    discovered = set(frame.columns)
    print("\nCanonical archive columns absent from sample:")
    print(", ".join(sorted(CANONICAL_ARCHIVE_COLUMNS - discovered)) or "(none)")
    print("Live columns omitted by the archive script's canonical schema:")
    print(", ".join(sorted(discovered - CANONICAL_ARCHIVE_COLUMNS)) or "(none)")
    inspect_lifecycles(frame, settings.top_values)


def resolve_targets(settings: Settings) -> dict[str, list[str]]:
    """Return unique Influx hosts mapped to the books expected on each host."""
    if settings.books:
        books = list(settings.books)
        host_by_book = {
            book: settings.host_override or resolve_book_host(book) for book in books
        }
    else:
        registry_hosts = _load_book_hosts()
        books = sorted(set(registry_hosts) | set(BOOKS))
        if not books:
            raise RuntimeError("The UMM location registry returned no books")
        host_by_book = {}
        for book in books:
            spec = book_spec(book)
            host_by_book[book] = (
                spec.host
                or registry_hosts.get(book)
                or f"{book.lower().replace('_', '-')}.jstcapdev.com"
            )

    targets: dict[str, list[str]] = defaultdict(list)
    for book in books:
        targets[host_by_book[book]].append(book)
    return dict(sorted(targets.items()))


def database_pattern_for_books(settings: Settings, books: Iterable[str]) -> str:
    if settings.database_pattern:
        return settings.database_pattern
    roots = sorted({book_spec(book).db or book for book in books}, key=len, reverse=True)
    alternatives = "|".join(re.escape(root) for root in roots)
    return rf"^(?:{alternatives})(?:_|$)"


def run(settings: Settings) -> int:
    targets = resolve_targets(settings)
    heading("Influx host inventory")
    render_table(
        pd.DataFrame(
            [
                {"host": host, "books": ", ".join(books), "book_count": len(books)}
                for host, books in targets.items()
            ]
        )
    )

    all_schema = []
    found_locations = []
    failed_hosts = []
    for host, books in targets.items():
        client = make_client(settings, host)
        try:
            print(
                f"\nConnecting to {host}:{settings.port} for {', '.join(books)} ...",
                flush=True,
            )
            client.ping()
            pattern = database_pattern_for_books(settings, books)
            databases, discovery = discover_databases(client, settings, pattern)
            if not discovery.empty:
                discovery.insert(0, "host", host)
                discovery.insert(1, "books", ", ".join(books))
                render_table(discovery)
            for database in databases:
                found_locations.append({"host": host, "database": database})

            schema = inspect_schema(client, databases)
            if not schema.empty:
                schema.insert(0, "host", host)
                all_schema.append(schema)
            for database in databases:
                analyze_database(client, database, settings)
        except Exception as exc:
            failed_hosts.append(
                {"host": host, "books": ", ".join(books), "error": f"{type(exc).__name__}: {exc}"}
            )
            print(f"Host failed; continuing: {host}: {exc}", file=sys.stderr)
        finally:
            client.close()

    heading("Global order_lifetime inventory")
    render_table(pd.DataFrame(found_locations))
    print("\nUnreachable/failed hosts:")
    render_table(pd.DataFrame(failed_hosts))
    if not found_locations:
        print(f"No reachable database contained {MEASUREMENT!r}.", file=sys.stderr)
        return 2

    schema = pd.concat(all_schema, ignore_index=True)
    heading("Global tag and field schema")
    render_table(schema.sort_values(["key", "host", "database", "kind"]))
    collisions = (
        schema.groupby(["host", "database", "key"])["kind"]
        .nunique()
        .reset_index(name="kind_count")
    )
    collisions = collisions[collisions["kind_count"] > 1]
    print("\nNames defined as both tags and fields:")
    render_table(collisions)

    definitions: dict[str, set[str]] = defaultdict(set)
    for row in schema.itertuples(index=False):
        definitions[row.key].add(f"{row.kind}/{row.type}")
    inconsistent = pd.DataFrame(
        [
            {"key": key, "definitions": ", ".join(sorted(values))}
            for key, values in definitions.items()
            if len(values) > 1
        ]
    )
    print("\nKeys whose definitions differ across all books:")
    render_table(inconsistent)
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    settings = parse_args(argv)
    try:
        return run(settings)
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        print(
            "Hint: verify VPN access and the UMM location registry. Use --book "
            "for a targeted retry, with --host only when its routing needs an override.",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
