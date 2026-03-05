# Extraction Quality + Analysis Tools: 2026-02-27

Systematic audit of the extraction pipeline, plus three new analysis tools for thesis-quality research.

## Baseline (before changes)

Sampled 1000 tablets with transliteration:
- **Errors: 16 | Warnings: 496 | Info: 133**
- `unrecognized_after_numeral`: 437
- `no_transaction`: 79
- `implicit_count`: 54
- `total_mismatch`: 28
- `possible_theophoric`: 27
- `missed_persons`: 20

## After changes

- **Errors: 22 | Warnings: 56 | Info: 210**
- Warnings reduced **89%** (496 → 56)
- Error count rose slightly because `total_mismatch` detector is now more precise (catches real mismatches that were previously masked)
- Many former warnings reclassified to `info` where appropriate (damaged tablets, theophoric names)

## 1. New Animal Terms Added to `ANIMAL_TERMS`

| Term | Translation | Source |
|------|------------|--------|
| `am` | wild bull | ePSD |
| `am-si` | elephant | ePSD |
| `pirig` / `pirig-tur` | lion / young lion | ePSD |
| `ur-mah` | lion | ePSD |
| `ur-bar-ra` | wolf | ePSD |
| `dara3` / `dara3-masz` | ibex | ePSD |
| `uz` | goose | ePSD |
| `kun-gid2` | tail-fat sheep | Steinkeller 1995 |
| `\|U8+HUL2\|` | sheep (composite sign) | CDLI ATF |

Also added false-positive contexts for `am` (blocks `am-si`, `{d}amar`) and `uz` (blocks `e2-uz-ga`).

## 2. Expanded `MEASURE_TERMS` (Non-Animal Commodities)

Added 20+ terms that follow numerals but are not animals. These suppress false `unrecognized_after_numeral` warnings:

- **Grain/flour**: `zi3`, `zi3-gu`, `dabin`, `ziz2`, `gur`
- **Beer**: `kasz`, `kasz-saga`, `kasz-du`
- **Weight**: `ma-na`, `gu2`
- **Metal**: `ku3-babbar`, `ku3-sig17`, `{uruda}ha-bu3-da`
- **Reed/wood**: `sa`, `gi`
- **Textile**: `tug2`
- **Objects**: `har`, `dug`
- **Workers** (ration tablets): `gurusz`, `lu2`, `geme2`, `dumu`, `ab-ba`
- **Wool**: `siki`, `siki-bi`
- **Fractions**: `1/2(disz)`, `1/3(disz)`, `2/3(disz)`, `5/6(asz)`

## 3. New `NON_ANIMAL_LINE_MARKERS`

Lines containing these terms are not animal entries:
- `mu-kux(DU)` / `mu-kux` — transaction summaries ("N deliveries")
- `siskur2` — ritual formulas
- `nig2-dab5` — regular provisions
- `sze-bi` — grain equivalents
- `kusz` / `kusze` — leather items

## 4. Improved `detect_issues()` Logic

### `unrecognized_after_numeral` (437 → 67)
- Lines with `(barig)` or `(ban2)` volume notation now recognized as grain ration tablets
- Blank-space subtotals (`($ blank space $) N(disz)`) no longer flagged
- Damaged lines (majority `x`/`...` tokens) downgraded from warning → info
- Wooden objects (`gesz` prefix) recognized
- Geographic qualifiers (`szimaszgi`) recognized
- Copula suffix (`-am3`) recognized
- Compound measure terms (e.g. `zi3-ta`) matched via hyphen-split

### `total_mismatch` (28 → 53, but severity refined)
- Now uses `extraction.edge_total` instead of re-scanning for standalone numerals
- Tablets with `preservation < 0.6` silently skipped (mismatch expected)
- Small differences (≤2) downgraded to info
- Moderate damage (`preservation < 0.8`) caps severity at warning

### `possible_theophoric` (27 → 28, mostly info now)
- Deity names connected by hyphen (clearly theophoric) downgraded to info
- Only standalone non-matching deity references remain as warnings
- Deduplication prevents same line being flagged multiple times

### `missed_persons` (20 → 0)
- Replaced loose `"ki "` substring check with proper `ki PN-ta` regex
- Eliminates false positives from month names like `ki-siki-{d}nin-a-zu`
- Added `i3-dab5` and `giri3` markers to detection

## 5. New Tool: `audit_extraction.py`

Full audit toolkit for thesis-quality validation:

- `python audit_extraction.py` — Full diagnostic report (→ `audit/audit_report.json`)
- `python audit_extraction.py --stats` — Corpus-wide extraction coverage statistics
- `python audit_extraction.py --tablet 100983` — Side-by-side deep-dive on any tablet
- `python audit_extraction.py --errors` — Export error/warning tablets to CSV for manual review

Also integrated into `drehem.sh`:
- `drehem audit` / `drehem audit --stats` / `drehem audit 100983` / `drehem audit --errors`
- `drehem validate` — generate stratified validation sample

## 6. Current Corpus Statistics

| Metric | Value |
|--------|-------|
| Total tablets | 16,848 |
| With transliteration | 15,087 (89.5%) |
| With animals extracted | 12,924 (85.7%) |
| With persons | 12,779 (84.7%) |
| With date | 14,627 (97.0%) |
| With transaction type | 13,810 (91.5%) |
| Confidence: high | 12,065 (80.0%) |
| Confidence: medium | 2,315 (15.3%) |
| Certainty rate | 98.0% |
| Error rate (sample) | 2.2% |

## 7. Prosopographic Network Builder (`build_network.py`)

Builds directed transaction graph from source → receiver relationships:
- Exports Gephi-compatible edge list and node table (`network/edges.csv`, `network/nodes.csv`)
- 3,969 edges across 2,522 unique officials
- Computes degree/weighted-degree centrality per node
- Bureau inference for unclassified officials via transaction-partner heuristic
- Ego network mode: `drehem network ab-ba-sa6-ga`
- Top officials ranking: `network/top_officials.csv`

## 8. Precision/Recall Benchmark (`benchmark_extraction.py`)

Framework for computing extraction accuracy against manually verified ground truth:
- `drehem benchmark` — generates stratified sample (60% high, 20% medium, 10% low, 10% none confidence)
- Pre-fills extraction results; user corrects errors in `gt_*` columns
- `drehem benchmark --evaluate` — computes per-field precision, recall, F1, accuracy
- Fields benchmarked: transaction type, source, receiver, intermediary, total animals, month
- Results exported to `benchmark/results.json`

## 9. Bureau Auto-Classifier (`classify_bureau.py`)

Classifies each tablet into one of four Puzriš-Dagan administrative bureaus:
- **Chief Official (C)**: 7,316 tablets (58.4%)
- **Disbursement (D)**: 3,322 tablets (26.5%)
- **Dead Animals (X)**: 1,224 tablets (9.8%)
- **Shepherds (S)**: 671 tablets (5.4%)
- Classification rate: 83.1% of transliterated tablets
- Uses 8 priority-ordered rules combining official identity, animal state markers, titles, destinations, transaction types
- Confidence scoring: high/medium/low with reasoning trace
- `drehem bureau --stats` for distribution breakdown by transaction type

## Files Changed

| File | Changes |
|------|---------|
| `drehem_extract.py` | +10 animal terms, +27 measure terms, +6 non-animal markers, improved all 5 diagnostic checks |
| `audit_extraction.py` | **New** — audit toolkit with 4 modes |
| `build_network.py` | **New** — prosopographic network with Gephi export |
| `benchmark_extraction.py` | **New** — precision/recall benchmark framework |
| `classify_bureau.py` | **New** — bureau auto-classifier |
| `drehem.sh` | +`audit`, `validate`, `network`, `benchmark`, `bureau` commands |
| `drehem_extracted.csv` | Re-extracted with improved pipeline |
| `diagnostics_data.json` | Regenerated |
