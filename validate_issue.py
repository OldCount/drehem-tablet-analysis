#!/usr/bin/env python3
"""
validate_issue.py — quick one-shot debugger for drehem_extract.py
===================================================================
Use this when you find a new edge case to see exactly what the
extractor currently does, WITHOUT running the full test suite.

Usage:
    python3 validate_issue.py P123525
    python3 validate_issue.py 123525
    python3 validate_issue.py --atf "3(disz) ad7 gu4"
    python3 validate_issue.py P123484 --show-lines
    python3 validate_issue.py P123525 --add-issue "ad7 carcass not counted"

Options:
    --show-lines    Print what extract_content_lines() sees
    --add-issue     Append a one-liner description to known_issues.md
    --no-names      Skip ORACC name normalization (faster startup)
"""

import argparse
import sys
import json
import datetime
from pathlib import Path

HERE    = Path(__file__).parent
DB_JSON = HERE / "tablet_data.json"
ISSUES  = HERE / "validation" / "known_issues.md"

# Prevent slow ORACC dictionary load during debugging
import drehem_extract as _de
_de._HAS_NAME_DICT = False

from drehem_extract import (
    extract_tablet, extract_content_lines, strip_atf_damage,
    ANIMAL_TERMS, CARCASS_MARKERS, DERIVED_COMMODITY_MARKERS,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_db() -> dict:
    print(f"  Loading {DB_JSON.name}...", end=" ", flush=True)
    db = json.loads(DB_JSON.read_text())
    print(f"{len(db):,} tablets.")
    return db


def resolve_id(raw: str) -> str:
    """Accept P123525, 123525, or p123525 and return the bare numeric ID."""
    return raw.lstrip("Pp").lstrip("0") or "0"


def run_on_tablet(tid: str, db: dict, show_lines: bool = False) -> None:
    t = db.get(tid)
    if not t:
        print(f"  ✗ Tablet {tid} not found in {DB_JSON.name}")
        sys.exit(1)

    atf = t.get("atf", "")
    date_str = f"{t.get('rul','')}.{t.get('yr','')}.{t.get('mo','')}.{t.get('dy','')}"
    des = t.get("des", "")

    print(f"\n  Tablet:  P{tid}  ({des})")
    print(f"  Date:    {date_str}")
    print()

    if show_lines:
        lines = extract_content_lines(atf)
        print("  ── extract_content_lines() output ───────────────────")
        for i, ln in enumerate(lines):
            cleaned = strip_atf_damage(ln)
            print(f"  {i:>3}.  raw: {ln!r}")
            print(f"        cln: {cleaned!r}")
        print()

    result = extract_tablet(tid, atf, date_str)
    _print_result(result, atf)


def run_on_atf(snippet: str, show_lines: bool = False) -> None:
    # Wrap bare snippet in minimal ATF envelope so extract_content_lines works
    if not snippet.strip().startswith("@"):
        snippet = f"@tablet\n@obverse\n1. {snippet}"

    print(f"\n  ATF snippet: {snippet!r}")
    print()

    if show_lines:
        lines = extract_content_lines(snippet)
        print("  ── extract_content_lines() output ───────────────────")
        for i, ln in enumerate(lines):
            cleaned = strip_atf_damage(ln)
            print(f"  {i:>3}.  raw: {ln!r}")
            print(f"        cln: {cleaned!r}")
        print()

    result = extract_tablet("snippet", snippet, "")
    _print_result(result, snippet)


def _print_result(result, atf: str) -> None:
    print("  ── Extraction result ─────────────────────────────────")
    print(f"  transaction_type : {result.transaction_type!r}")
    print(f"  total_animals    : {result.total_animals}")
    print(f"  month_number     : {result.month_number}")
    print(f"  regnal_year      : {result.regnal_year}")
    print(f"  ruler            : {result.ruler!r}")
    print()

    if result.animals:
        print("  Animals:")
        for a in result.animals:
            qs = ", ".join(a.qualifiers) if a.qualifiers else "—"
            dmg = " [DAMAGED]" if a.damaged else ""
            print(f"    {a.count:>4}×  {a.animal:<25}  qualifiers: {qs}{dmg}")
            print(f"          raw: {a.raw!r}")
    else:
        print("  Animals: (none extracted)")

    print()

    if result.persons:
        print("  Persons:")
        for p in result.persons:
            print(f"    {p.role:<15}  {p.name}  [{p.title or '—'}]")
    else:
        print("  Persons: (none)")

    print()
    print("  ── Raw ATF ──────────────────────────────────────────")
    for line in atf.splitlines():
        print(f"  {line}")
    print()


def append_known_issue(tablet_id: str | None, description: str) -> None:
    ISSUES.parent.mkdir(parents=True, exist_ok=True)
    today = datetime.date.today().isoformat()
    ref = f"P{tablet_id}" if tablet_id else "snippet"

    entry = (
        f"\n## [{today}] {ref} — {description}\n"
        f"- **ATF**: `???`\n"
        f"- **Problem**: {description}\n"
        f"- **Status**: Open\n"
        f"- **Expected**: `???`\n"
        f"- **Ground truth entry added**: No\n"
    )

    with open(ISSUES, "a") as f:
        f.write(entry)

    print(f"\n  ✓ Issue appended to {ISSUES.relative_to(HERE)}")
    print(f"    Fill in the ATF and expected values in that file.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Quick extractor debugger — shows what drehem_extract.py does on one tablet or ATF snippet."
    )
    parser.add_argument("tablet_id", nargs="?",
                        help="Tablet ID (P123525, 123525, …)")
    parser.add_argument("--atf",        help="Raw ATF snippet to test directly")
    parser.add_argument("--show-lines", action="store_true",
                        help="Print extract_content_lines() output")
    parser.add_argument("--add-issue",  metavar="DESC",
                        help="Append a bug entry to known_issues.md")

    args = parser.parse_args()

    if not args.tablet_id and not args.atf:
        parser.print_help()
        sys.exit(0)

    print("\nvalidate_issue.py — drehem extractor debugger")
    print("=" * 54)

    if args.atf:
        run_on_atf(args.atf, show_lines=args.show_lines)
        if args.add_issue:
            append_known_issue(None, args.add_issue)
    else:
        tid = resolve_id(args.tablet_id)
        db = load_db()
        run_on_tablet(tid, db, show_lines=args.show_lines)
        if args.add_issue:
            append_known_issue(tid, args.add_issue)


if __name__ == "__main__":
    main()
