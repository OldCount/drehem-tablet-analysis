#!/usr/bin/env bash
# drehem.sh - Smart launcher for the Drehem extraction pipeline
#
# Freshness checks: compares output file timestamps against source files.
# Up-to-date outputs are skipped unless you pass --force.
#
# Usage (from any folder if alias is installed):
#   drehem dict              Rebuild name dictionary if stale
#   drehem extract           Re-extract if drehem_extract.py or DB changed
#   drehem search            Serve name_search.html on port 8090
#   drehem dashboard         Serve diagnostics dashboard on port 8787
#   drehem status            Show freshness of all outputs
#   drehem all               Run all stale steps, then open studio (dashboard.html)
#   drehem dict --force      Force rebuild even if up to date
#   drehem setup             Add 'drehem' alias to ~/.zshrc

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SEARCH_PORT=8090
DASH_PORT=8787
PYTHON=python3

# ── Output files we track ─────────────────────────────────────────────────
DICT_OUT="$SCRIPT_DIR/oracc_name_dictionary.json"
EXTRACT_OUT="$SCRIPT_DIR/drehem_extracted.csv"
DIAG_OUT="$SCRIPT_DIR/diagnostics_data.json"

# ── Source files for freshness checks ─────────────────────────────────────
DICT_SOURCES=("$SCRIPT_DIR/build_name_dictionary.py")
EXTRACT_SOURCES=(
    "$SCRIPT_DIR/drehem_extract.py"
    "$SCRIPT_DIR/drehem_database.csv"
    "$DICT_OUT"
)
DIAG_SOURCES=(
    "$SCRIPT_DIR/build_diagnostics.py"
    "$EXTRACT_OUT"
)

# ── Freshness helpers ──────────────────────────────────────────────────────
is_fresh() {
    # is_fresh OUTPUT SOURCE... — true if OUTPUT exists and is newer than all SOURCEs
    local out="$1"; shift
    [[ -f "$out" ]] || return 1
    for src in "$@"; do
        [[ -f "$src" ]] || continue
        [[ "$out" -nt "$src" ]] || return 1
    done
    return 0
}

status_line() {
    local label="$1" fresh="$2" path="$3"
    local age=""
    if [[ -f "$path" ]]; then
        # How many minutes ago was it last modified?
        local mtime now
        mtime=$(stat -f "%m" "$path" 2>/dev/null || echo 0)
        now=$(date +%s)
        local mins=$(( (now - mtime) / 60 ))
        if (( mins < 60 )); then
            age="${mins}m ago"
        elif (( mins < 1440 )); then
            age="$(( mins / 60 ))h ago"
        else
            age="$(( mins / 1440 ))d ago"
        fi
    fi
    if [[ "$fresh" == "yes" ]]; then
        printf "  %-12s  up to date   (%s)\n" "$label" "$age"
    elif [[ -f "$path" ]]; then
        printf "  %-12s  STALE        (%s)\n" "$label" "$age"
    else
        printf "  %-12s  missing\n" "$label"
    fi
}

# ── Main ──────────────────────────────────────────────────────────────────
cmd="${1:-status}"
shift || true
FORCE=0
for arg in "$@"; do [[ "$arg" == "--force" ]] && FORCE=1; done

case "$cmd" in
  status)
    echo "Drehem pipeline status:"
    echo ""
    if is_fresh "$DICT_OUT" "${DICT_SOURCES[@]}"; then
        status_line "dictionary" "yes" "$DICT_OUT"
    else
        status_line "dictionary" "no" "$DICT_OUT"
    fi
    if is_fresh "$EXTRACT_OUT" "${EXTRACT_SOURCES[@]}"; then
        status_line "extraction" "yes" "$EXTRACT_OUT"
    else
        status_line "extraction" "no" "$EXTRACT_OUT"
    fi
    if is_fresh "$DIAG_OUT" "${DIAG_SOURCES[@]}"; then
        status_line "diagnostics" "yes" "$DIAG_OUT"
    else
        status_line "diagnostics" "no" "$DIAG_OUT"
    fi
    echo ""
    echo "Commands: dict extract search dashboard visualize audit validate network benchmark bureau all backup setup"
    ;;

  dict)
    if [[ $FORCE -eq 0 ]] && is_fresh "$DICT_OUT" "${DICT_SOURCES[@]}"; then
        echo "Dictionary is up to date. Use --force to rebuild."
    else
        echo "Building name dictionary..."
        $PYTHON "$SCRIPT_DIR/build_name_dictionary.py"
    fi
    ;;

  extract)
    if [[ $FORCE -eq 0 ]] && is_fresh "$EXTRACT_OUT" "${EXTRACT_SOURCES[@]}"; then
        echo "Extraction is up to date. Use --force to re-extract."
    else
        echo "Running extraction..."
        $PYTHON "$SCRIPT_DIR/drehem_extract.py"
        echo "Done -> drehem_extracted.csv"
    fi
    ;;

  search)
    # Kill any existing server on the port first so reload is instantaneous
    lsof -ti tcp:"$SEARCH_PORT" | xargs kill -9 2>/dev/null || true
    echo "Serving name_search.html on port $SEARCH_PORT"
    echo "URL: http://localhost:$SEARCH_PORT/name_search.html"
    echo "Press Ctrl+C to stop."
    (sleep 0.4 && open "http://localhost:$SEARCH_PORT/name_search.html") &
    $PYTHON -m http.server "$SEARCH_PORT" --directory "$SCRIPT_DIR"
    ;;

  dashboard)
    lsof -ti tcp:"$DASH_PORT" | xargs kill -9 2>/dev/null || true
    echo "Starting diagnostics dashboard on port $DASH_PORT"
    echo "URL: http://localhost:$DASH_PORT"
    echo "Press Ctrl+C to stop."
    $PYTHON "$SCRIPT_DIR/run_dashboard.py" --port "$DASH_PORT"
    ;;

  visualize|vis)
    VIS_PORT=8585
    lsof -ti tcp:"$VIS_PORT" | xargs kill -9 2>/dev/null || true
    echo "Starting tablet visualizer on port $VIS_PORT"
    echo "URL: http://localhost:$VIS_PORT/tablet_vis.html"
    echo "Press Ctrl+C to stop."
    (sleep 0.4 && open "http://localhost:$VIS_PORT/tablet_vis.html") &
    $PYTHON "$SCRIPT_DIR/tablet_visualizer.py" --port "$VIS_PORT"
    ;;

  all)
    echo "Checking pipeline status..."
    if [[ $FORCE -eq 1 ]] || ! is_fresh "$DICT_OUT" "${DICT_SOURCES[@]}"; then
        echo "Step 1/2: Building name dictionary..."
        $PYTHON "$SCRIPT_DIR/build_name_dictionary.py"
    else
        echo "Step 1/2: Dictionary is current, skipping."
    fi

    if [[ $FORCE -eq 1 ]] || ! is_fresh "$EXTRACT_OUT" "${EXTRACT_SOURCES[@]}"; then
        echo "Step 2/3: Running extraction..."
        $PYTHON "$SCRIPT_DIR/drehem_extract.py"
    else
        echo "Step 2/3: Extraction is current, skipping."
    fi

    if [[ $FORCE -eq 1 ]] || ! is_fresh "$DIAG_OUT" "${DIAG_SOURCES[@]}"; then
        echo "Step 3/3: Building dashboard JSON..."
        $PYTHON "$SCRIPT_DIR/build_diagnostics.py"
    else
        echo "Step 3/3: Dashboard JSON is current, skipping."
    fi

    lsof -ti tcp:"$SEARCH_PORT" | xargs kill -9 2>/dev/null || true
    echo "Serving studio on port $SEARCH_PORT"
    echo "URL: http://localhost:$SEARCH_PORT/dashboard.html"
    echo "Press Ctrl+C to stop."
    (sleep 0.4 && open "http://localhost:$SEARCH_PORT/dashboard.html") &
    $PYTHON -m http.server "$SEARCH_PORT" --directory "$SCRIPT_DIR"
    ;;

  setup)
    ZSHRC="$HOME/.zshrc"
    MARKER="# drehem alias"
    if grep -q "$MARKER" "$ZSHRC" 2>/dev/null; then
        echo "Alias already installed in $ZSHRC"
    else
        cat >> "$ZSHRC" <<EOF

$MARKER
drehem() { bash "$SCRIPT_DIR/drehem.sh" "\$@"; }
EOF
        echo "Alias added to $ZSHRC"
        echo "Run 'source ~/.zshrc' or open a new terminal to activate."
    fi
    ;;

  audit)
    echo "Running extraction audit..."
    if [[ "${2:-}" == "--errors" ]]; then
        $PYTHON "$SCRIPT_DIR/audit_extraction.py" --errors
    elif [[ "${2:-}" == "--stats" ]]; then
        $PYTHON "$SCRIPT_DIR/audit_extraction.py" --stats
    elif [[ "${2:-}" =~ ^P?[0-9]+$ ]]; then
        $PYTHON "$SCRIPT_DIR/audit_extraction.py" --tablet "${2}"
    else
        $PYTHON "$SCRIPT_DIR/audit_extraction.py"
    fi
    ;;

  validate)
    echo "Generating validation sample..."
    $PYTHON "$SCRIPT_DIR/validate_sample.py" "$@"
    ;;

  network)
    echo "Building prosopographic network..."
    if [[ -n "${2:-}" ]]; then
        $PYTHON "$SCRIPT_DIR/build_network.py" --person "${2}"
    else
        $PYTHON "$SCRIPT_DIR/build_network.py"
    fi
    ;;

  benchmark)
    if [[ "${2:-}" == "--evaluate" ]]; then
        $PYTHON "$SCRIPT_DIR/benchmark_extraction.py" --evaluate
    else
        $PYTHON "$SCRIPT_DIR/benchmark_extraction.py" --generate ${@:2}
    fi
    ;;

  bureau)
    if [[ "${2:-}" == "--stats" ]]; then
        $PYTHON "$SCRIPT_DIR/classify_bureau.py" --stats
    else
        $PYTHON "$SCRIPT_DIR/classify_bureau.py"
    fi
    ;;

  backup)
    STAMP=$(date +"%Y-%m-%d_%H-%M")
    DEST="$SCRIPT_DIR/backups/$STAMP"
    mkdir -p "$DEST"
    # Copy everything except large CSVs, cache dirs, and the backups folder itself
    rsync -a \
      --exclude="*.csv" \
      --exclude="backups/" \
      --exclude="__pycache__/" \
      --exclude=".oracc_cache/" \
      --exclude="*.pyc" \
      "$SCRIPT_DIR/" "$DEST/"
    echo "Backup saved -> $DEST"
    echo "Contents:"
    ls -lh "$DEST"
    ;;

  help|--help|-h)
    echo "Usage: drehem <command> [--force]"
    echo ""
    echo "  status      Show freshness of all pipeline outputs"
    echo "  dict        Rebuild name dictionary (skips if current)"
    echo "  extract     Re-extract tablets (skips if current)"
    echo "  search      Serve name_search.html on port $SEARCH_PORT"
    echo "  dashboard   Serve diagnostics dashboard on port $DASH_PORT"
    echo "  visualize   Interactive tablet annotation visualizer on port 8585"
    echo "  audit       Run extraction audit (--stats, --errors, or TABLET_ID)"
    echo "  validate    Generate stratified validation sample"
    echo "  network     Build prosopographic network (or --person NAME for ego net)"
    echo "  benchmark   Generate/evaluate precision-recall benchmark"
    echo "  bureau      Classify tablets by administrative bureau (--stats)"
    echo "  all         Run stale steps then open dashboard"
    echo "  backup      Snapshot all scripts to backups/YYYY-MM-DD_HH-MM/"
    echo "  setup       Install 'drehem' command globally via ~/.zshrc"
    echo ""
    echo "  --force     Force re-run even if output is current"
    ;;

  *)
    echo "Unknown command: $cmd. Run 'drehem help' for usage."
    exit 1
    ;;
esac
