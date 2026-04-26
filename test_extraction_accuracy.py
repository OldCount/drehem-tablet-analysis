#!/usr/bin/env python3
"""
Critical accuracy test for drehem_extract.py
=============================================
Tests the extraction pipeline against hand-verified ground truth.

Three test suites:
  1. GOLD STANDARD  — manually translated tablets from the Obsidian vault
  2. EDGE CASES     — documented difficult patterns from CLAUDE.md
  3. REGRESSION     — 100 high-confidence corpus tablets (catches silent regressions)

Usage:
    python3 test_extraction_accuracy.py              # all suites, summary only
    python3 test_extraction_accuracy.py --verbose    # show field-level diffs
    python3 test_extraction_accuracy.py --suite gold # run one suite only
    python3 test_extraction_accuracy.py --report     # write results/test_report_<date>.json

Exit code 0 = all tests passed; 1 = failures detected.
"""

import argparse
import json
import sys
import os
import csv
import datetime
from pathlib import Path
from dataclasses import asdict

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
HERE        = Path(__file__).parent
DATA_JSON   = HERE / "tablet_data.json"
GROUND_TRUTH= HERE / "validation" / "ground_truth.json"
RESULTS_DIR = HERE / "validation"

# ---------------------------------------------------------------------------
# Import extractor
# ---------------------------------------------------------------------------
sys.path.insert(0, str(HERE))
from drehem_extract import extract_tablet

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_db() -> dict:
    with open(DATA_JSON) as f:
        return json.load(f)


def load_ground_truth() -> dict:
    with open(GROUND_TRUTH) as f:
        return json.load(f)


def get_person(result, role: str) -> str:
    """Return first person with given role, or empty string."""
    for p in result.persons:
        if p.role == role:
            return p.name
    return ""


def get_all_persons(result, role: str) -> list[str]:
    """Return all persons with given role (for multi-sender/receiver tests)."""
    return [p.name for p in result.persons if p.role == role]


def compare_field(field: str, got, expected, strict: bool = False) -> tuple[bool, str]:
    """
    Compare a single extracted field against the expected value.
    Returns (passed, message).

    None in expected means 'not tested'.
    Strings are compared case-insensitively unless strict=True.
    Lists (e.g. divine_recipients) are compared order-insensitively.
    """
    if expected is None:
        return True, "skipped"

    # List comparison must come before the falsy check — an empty list is
    # falsy in Python, so [] == [] would otherwise be misreported as a failure.
    if isinstance(expected, list):
        got_list = got if isinstance(got, list) else ([] if not got else [got])
        norm = lambda lst: sorted(str(x).lower().strip() for x in lst)
        ok = norm(got_list) == norm(expected)
        msg = "ok" if ok else f"got {got_list!r}, expected {expected!r}"
        return ok, msg

    if got is None or got == "" or got == 0 and expected != 0:
        if expected in (None, "", 0):
            return True, "ok"
        return False, f"got empty/None, expected {expected!r}"

    if isinstance(expected, str):
        g = str(got).strip()
        e = expected.strip()
        if not strict:
            g, e = g.lower(), e.lower()
        ok = g == e
        msg = "ok" if ok else f"got {got!r}, expected {expected!r}"
        return ok, msg

    if isinstance(expected, int):
        ok = int(got) == expected
        msg = "ok" if ok else f"got {got}, expected {expected}"
        return ok, msg

    if isinstance(expected, bool):
        ok = bool(got) == expected
        msg = "ok" if ok else f"got {got!r}, expected {expected!r}"
        return ok, msg

    return got == expected, f"got {got!r}, expected {expected!r}"


FIELDS_TO_TEST = [
    "transaction_type",
    "source",         # str for single sender, list for multi-sender tablets
    "receiver",       # str for single receiver, list for multi-receiver tablets
    "commissioner",
    "intermediary",
    "deliverer",
    "total_animals",
    "source_office",
    "receiver_office",
    "month_number",
    "regnal_year",
    "ruler",
    "destination",
    "destination_category",
    "divine_recipients",
]


def run_single_tablet(tablet_id: str, db: dict) -> dict | None:
    """Extract from ATF; return None if tablet not in DB."""
    if tablet_id not in db:
        return None
    t = db[tablet_id]
    date_str = f"{t.get('rul','')}.{t.get('yr','')}.{t.get('mo','')}.{t.get('dy','')}"
    return extract_tablet(tablet_id, t.get("atf", ""), date_str)


def evaluate_tablet(tablet_id: str, expected_fields: dict, result, verbose: bool) -> dict:
    """Compare extraction result against expected fields. Returns a result dict."""
    passed_fields = []
    failed_fields = []
    skipped_fields = []

    for field in FIELDS_TO_TEST:
        exp = expected_fields.get(field)

        # When expected is a list, use get_all_persons for multi-person fields
        # so ground truth can test full sender/receiver sets.
        if field in ("source", "receiver", "commissioner", "intermediary", "deliverer"):
            if isinstance(exp, list):
                got = get_all_persons(result, field)
            else:
                got = get_person(result, field)
        else:
            got = getattr(result, field, None)

        ok, msg = compare_field(field, got, exp)

        if exp is None:
            skipped_fields.append(field)
        elif ok:
            passed_fields.append(field)
        else:
            failed_fields.append((field, msg))
            if verbose:
                print(f"    ✗ {field}: {msg}")

    return {
        "tablet_id": tablet_id,
        "passed": len(failed_fields) == 0,
        "passed_fields": passed_fields,
        "failed_fields": failed_fields,
        "skipped_fields": skipped_fields,
    }


# ===========================================================================
# Suite 1 + 2: Ground truth (gold standard + edge cases)
# ===========================================================================

def run_ground_truth_suite(db: dict, suite_filter: str | None, verbose: bool) -> list[dict]:
    """Run all tablets from ground_truth.json."""
    gt = load_ground_truth()
    results = []

    source_labels = {
        "manual_translation": "GOLD",
        "cdli":               "CDLI",
        "edge_case":          "EDGE",
    }

    for tablet in gt["tablets"]:
        tid   = tablet["id"]
        des   = tablet["designation"]
        src   = tablet["source"]
        label = source_labels.get(src, src.upper())
        note  = tablet.get("note", "")

        # Suite filtering
        if suite_filter == "gold" and src not in ("manual_translation", "cdli"):
            continue
        if suite_filter == "edge" and src != "edge_case":
            continue

        result = run_single_tablet(tid, db)
        if result is None:
            print(f"  [SKIP] P{tid} ({des}) — not in tablet_data.json")
            continue

        print(f"  [{label}] P{tid} ({des})")
        if note and verbose:
            print(f"         {note}")

        eval_result = evaluate_tablet(tid, tablet["expected"], result, verbose)
        eval_result["designation"] = des
        eval_result["source"] = src
        eval_result["note"] = note

        status = "PASS" if eval_result["passed"] else "FAIL"
        n_fail = len(eval_result["failed_fields"])
        n_pass = len(eval_result["passed_fields"])
        n_skip = len(eval_result["skipped_fields"])
        print(f"         → {status}  {n_pass} ok / {n_fail} fail / {n_skip} skip")
        results.append(eval_result)

    return results


# ===========================================================================
# Suite 3: Regression (snapshot-based)
# ===========================================================================

SNAPSHOT_FILE = RESULTS_DIR / "regression_snapshot.json"

# Transaction type aliases: these label changes are NOT regressions.
TYPE_ALIASES: dict[str, str] = {
    "receipt":     "to accept",
    "to accept":   "receipt",
    "szu ba-ti":   "to accept",
}


def _normalize_type(t: str) -> str:
    return TYPE_ALIASES.get(t.lower().strip(), t.lower().strip())


def build_regression_snapshot(db: dict, sample_size: int = 200) -> dict:
    """
    Run the extractor on a stratified sample of high-confidence tablets and
    save the results as the regression baseline snapshot.
    Call this once with --snapshot to establish or refresh the baseline.
    """
    # Stratify by ruler
    by_ruler: dict[str, list] = {}
    for tid, t in db.items():
        if t.get("conf") == "high" and t.get("atf"):
            ruler = t.get("rul", "unknown")
            by_ruler.setdefault(ruler, []).append(tid)

    total_high = sum(len(v) for v in by_ruler.values())
    sample_ids = []
    for ruler, ids in sorted(by_ruler.items()):
        n = max(1, round(len(ids) / total_high * sample_size))
        sample_ids.extend(ids[:n])
    sample_ids = sample_ids[:sample_size]

    snapshot = {}
    for tid in sample_ids:
        t = db[tid]
        date_str = f"{t.get('rul','')}.{t.get('yr','')}.{t.get('mo','')}.{t.get('dy','')}"
        result = extract_tablet(tid, t.get("atf", ""), date_str)
        snapshot[tid] = {
            "designation":      t.get("des", ""),
            "transaction_type": result.transaction_type,
            "source":           get_person(result, "source"),
            "receiver":         get_person(result, "receiver"),
            "total_animals":    result.total_animals,
            "source_office":    result.source_office,
            "receiver_office":  result.receiver_office,
        }

    with open(SNAPSHOT_FILE, "w") as f:
        json.dump({"generated": datetime.date.today().isoformat(),
                   "sample_size": len(snapshot),
                   "tablets": snapshot}, f, indent=2)
    return snapshot


def run_regression_suite(db: dict, verbose: bool, sample_size: int = 200) -> list[dict]:
    """
    Compare fresh extraction against the snapshot baseline.

    First run (no snapshot): auto-generates the baseline and reports 0 regressions.
    Subsequent runs: detects changes introduced since the snapshot was taken.
    """
    if not SNAPSHOT_FILE.exists():
        print(f"\n  No snapshot found — building baseline now ({sample_size} tablets)...")
        snapshot = build_regression_snapshot(db, sample_size)
        print(f"  Snapshot saved to {SNAPSHOT_FILE.name}")
        print(f"  → Baseline established. Re-run to compare against it.")
        return []

    with open(SNAPSHOT_FILE) as f:
        snap_data = json.load(f)

    snapshot    = snap_data["tablets"]
    snap_date   = snap_data.get("generated", "unknown")
    n_snap      = snap_data.get("sample_size", len(snapshot))
    print(f"\n  Comparing against snapshot from {snap_date} ({n_snap} tablets)...")

    results     = []
    regressions = []

    for tid, baseline in snapshot.items():
        if tid not in db:
            continue
        t = db[tid]
        date_str = f"{t.get('rul','')}.{t.get('yr','')}.{t.get('mo','')}.{t.get('dy','')}"
        result = extract_tablet(tid, t.get("atf", ""), date_str)

        got = {
            "transaction_type": result.transaction_type,
            "source":           get_person(result, "source"),
            "receiver":         get_person(result, "receiver"),
            "total_animals":    result.total_animals,
            "source_office":    result.source_office,
            "receiver_office":  result.receiver_office,
        }

        diffs = []
        for field, exp in baseline.items():
            if field == "designation":
                continue
            now = got.get(field, "")
            if not exp and not now:
                continue  # both empty — skip

            if field == "transaction_type":
                if _normalize_type(str(exp)) != _normalize_type(str(now)):
                    diffs.append(f"{field}: was={exp!r}, now={now!r}")
            elif field == "total_animals":
                if exp and now and int(exp) != int(now):
                    diffs.append(f"{field}: was={exp}, now={now}")
            else:
                e = str(exp).lower().strip()
                n = str(now).lower().strip()
                if e and n and e != n:
                    diffs.append(f"{field}: was={exp!r}, now={now!r}")

        record = {
            "tablet_id":   tid,
            "designation": baseline.get("designation", ""),
            "passed":      len(diffs) == 0,
            "diffs":       diffs,
        }
        results.append(record)

        if diffs:
            regressions.append(record)
            if verbose:
                print(f"    REGRESSION P{tid} ({baseline.get('designation', '')}): {'; '.join(diffs)}")

    n_pass = sum(1 for r in results if r["passed"])
    n_fail = len(regressions)
    print(f"  → {n_pass}/{len(results)} stable, {n_fail} regressions detected")
    if regressions and not verbose:
        print("    (run with --verbose to see details)")

    return results


# ===========================================================================
# Reporting
# ===========================================================================

def print_summary(gt_results: list[dict], reg_results: list[dict]):
    """Print a clean summary table."""
    print()
    print("=" * 60)
    print("  EXTRACTION ACCURACY TEST SUMMARY")
    print("=" * 60)

    # Ground truth breakdown by source type
    by_type: dict[str, list] = {}
    for r in gt_results:
        by_type.setdefault(r["source"], []).append(r)

    for src_type, records in by_type.items():
        label = {"manual_translation": "Gold Standard", "cdli": "CDLI Verified", "edge_case": "Edge Cases"}.get(src_type, src_type)
        n_pass = sum(1 for r in records if r["passed"])
        n_total = len(records)
        pct = 100 * n_pass / n_total if n_total else 0
        print(f"  {label:20s}  {n_pass}/{n_total} passed ({pct:.0f}%)")

    # Per-field accuracy across all ground truth
    field_pass: dict[str, int] = {f: 0 for f in FIELDS_TO_TEST}
    field_total: dict[str, int] = {f: 0 for f in FIELDS_TO_TEST}
    for r in gt_results:
        for f in r["passed_fields"]:
            field_pass[f] += 1
            field_total[f] += 1
        for f, _ in r["failed_fields"]:
            field_total[f] += 1

    print()
    print("  Per-field accuracy:")
    for field in FIELDS_TO_TEST:
        tot = field_total.get(field, 0)
        pas = field_pass.get(field, 0)
        if tot == 0:
            continue
        pct = 100 * pas / tot
        bar = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
        status = "✓" if pct == 100 else ("△" if pct >= 75 else "✗")
        print(f"    {status} {field:25s} {bar} {pas}/{tot} ({pct:.0f}%)")

    # Regression suite
    if reg_results:
        n_stable = sum(1 for r in reg_results if r["passed"])
        n_total = len(reg_results)
        pct = 100 * n_stable / n_total if n_total else 0
        print()
        print(f"  Regression suite:         {n_stable}/{n_total} stable ({pct:.0f}%)")

    # Failures
    all_failures = [r for r in gt_results if not r["passed"]]
    if all_failures:
        print()
        print("  ── FAILURES ──────────────────────────────────────────")
        for r in all_failures:
            tid = r["tablet_id"]
            des = r["designation"]
            print(f"  P{tid} ({des}):")
            for field, msg in r["failed_fields"]:
                print(f"    ✗ {field}: {msg}")

    reg_failures = [r for r in reg_results if not r["passed"]]
    if reg_failures:
        print()
        print("  ── REGRESSIONS ───────────────────────────────────────")
        for r in reg_failures[:10]:  # cap at 10 to avoid flooding
            print(f"  P{r['tablet_id']} ({r['designation']}): {'; '.join(r['diffs'])}")
        if len(reg_failures) > 10:
            print(f"  ... and {len(reg_failures)-10} more (use --report for full list)")

    print("=" * 60)

    total_pass = sum(1 for r in gt_results if r["passed"])
    total_gt = len(gt_results)
    total_stable = sum(1 for r in reg_results if r["passed"])
    total_reg = len(reg_results)

    all_ok = (total_pass == total_gt) and (len(reg_failures) == 0)
    overall = "ALL TESTS PASSED ✓" if all_ok else "SOME TESTS FAILED ✗"
    print(f"  {overall}")
    print(f"  Ground truth: {total_pass}/{total_gt}   Regression: {total_stable}/{total_reg}")
    print("=" * 60)

    return all_ok


def write_json_report(gt_results: list[dict], reg_results: list[dict]):
    """Write detailed JSON report."""
    today = datetime.date.today().isoformat()
    path = RESULTS_DIR / f"test_report_{today}.json"
    report = {
        "date": today,
        "ground_truth_results": gt_results,
        "regression_results": reg_results,
        "summary": {
            "gt_pass": sum(1 for r in gt_results if r["passed"]),
            "gt_total": len(gt_results),
            "reg_stable": sum(1 for r in reg_results if r["passed"]),
            "reg_total": len(reg_results),
        }
    }
    with open(path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\n  Report written → {path.name}")


# ===========================================================================
# Main
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="Drehem extraction accuracy test suite")
    parser.add_argument("--verbose",  "-v", action="store_true", help="Show field-level diffs")
    parser.add_argument("--suite",          choices=["gold", "edge", "regression", "all"], default="all",
                        help="Which suite to run (default: all)")
    parser.add_argument("--report",   "-r", action="store_true", help="Write JSON report file")
    parser.add_argument("--sample",         type=int, default=200,
                        help="Regression sample size (default: 200)")
    parser.add_argument("--snapshot",       action="store_true",
                        help="Rebuild the regression baseline snapshot from current extractor output")
    args = parser.parse_args()

    print()
    print("Drehem Extraction Accuracy Test")
    print(f"  tablet_data.json  : {DATA_JSON}")
    print(f"  ground_truth.json : {GROUND_TRUTH}")
    print()

    db = load_db()
    print(f"  Loaded {len(db):,} tablets from corpus.")
    print()

    # ── Snapshot rebuild (standalone action) ─────────────────────────────
    if args.snapshot:
        print(f"── Rebuilding regression snapshot ({args.sample} tablets) ──────")
        snap = build_regression_snapshot(db, args.sample)
        print(f"  Done. {len(snap)} tablets captured in {SNAPSHOT_FILE.name}")
        sys.exit(0)

    gt_results  = []
    reg_results = []

    # ── Ground truth suites ───────────────────────────────────────────────
    if args.suite in ("all", "gold", "edge"):
        suite_filter = None
        if args.suite == "gold":
            suite_filter = "gold"
            print("── Suite 1: Gold Standard + CDLI ──────────────────────────")
        elif args.suite == "edge":
            suite_filter = "edge"
            print("── Suite 2: Edge Cases ─────────────────────────────────────")
        else:
            print("── Suite 1+2: Ground Truth (Gold + CDLI + Edge Cases) ──────")

        gt_results = run_ground_truth_suite(db, suite_filter=suite_filter, verbose=args.verbose)

    # ── Regression suite ──────────────────────────────────────────────────
    if args.suite in ("all", "regression"):
        print("\n── Suite 3: Regression (corpus sample) ─────────────────────")
        reg_results = run_regression_suite(db, verbose=args.verbose, sample_size=args.sample)

    # ── Summary ───────────────────────────────────────────────────────────
    all_ok = print_summary(gt_results, reg_results)

    if args.report:
        write_json_report(gt_results, reg_results)

    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
