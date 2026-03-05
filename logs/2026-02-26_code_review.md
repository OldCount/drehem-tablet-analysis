# Code Review & Cleanup: 2026-02-26

Rigorous uniformity pass across all Python files in the extraction pipeline.

## 1. Removed Bloat

- **`build_name_dictionary.py`**: Removed the demo block in `main()` that printed sample normalizations and glossary stats. Also removed unused `BytesIO` import and a duplicate section header.

## 2. Consolidated Duplicated Code

- **`detect_issues()`**: Was copy-pasted between `build_diagnostics.py` and `run_dashboard.py` (~100 lines each). Moved the canonical version into `drehem_extract.py` where it belongs (uses extraction internals). Both diagnostic scripts now import it.
- **`build_diagnostics.py`**: Rewritten as a thin wrapper — imports `detect_issues` and `compute_confidence` from `drehem_extract.py`, writes `diagnostics_data.json`. No more duplicate logic.
- **`run_dashboard.py`**: Stripped the inline `detect_issues`, now imports from `drehem_extract.py`. Also removed unused imports: `re`, `threading`, and several extraction internals (`ANIMAL_TERMS`, `KNOWN_DEITIES`, `strip_atf_damage`, `has_damage`, `_is_standalone_deity`, `parse_numeral`, `is_szunigin_line`, `is_niginba_line`, `MEASURE_TERMS`).

## 3. Performance Optimizations (`drehem_extract.py`)

- **`SORTED_ANIMAL_TERMS`**: Pre-sorted at module level instead of re-sorting on every call to `extract_animals()` and `detect_issues()`.
- **`ADMIN_KEYWORDS`**: Moved from inside `extract_animals()` loop body to module-level constant.
- **`DAMAGED_NUMERAL_PATTERN`**: Precompiled at module level instead of re-compiling the regex on every iteration of the animal extraction loop.

## 4. Docstring & Comment Standardization

Applied uniformly across all files:
- Trimmed verbose docstrings to 1–3 lines max (many were 10–25 lines).
- Removed usage examples embedded in docstrings (e.g. `normalize_name`'s full code sample).
- Removed AI-sounding language and overly formal phrasing.
- Kept domain-specific Assyriology comments (bala, sza3-bi-ta, ensi2, etc.) intact.
- Standardized to double quotes across `extract_drehem.py`, `validate_sample.py`, `drehem_analysis.py`.
- Removed `import csv` (unused) from `drehem_analysis.py`.

## 5. Style Uniformity

- All files now use double-quote strings consistently.
- Docstrings are imperative mood ("Extract X from Y" not "This function extracts X from Y").
- No first-person ("I"/"my") in code comments — reserved for thesis prose only.
- No references to AI assistance or generation in any code file.

## 6. New Additions

### `drehem_analysis.py`
- **`analyze_herd_composition()`**: Species composition heatmaps broken down by source office and by ruler. Answers thesis sub-question 2 ("Does herd composition differ by bureau, official, or time period?"). Outputs `herd_composition.png`.
- **`corpus_summary()`**: Generates thesis-ready statistics (total tablets, transliteration rate, confidence distribution, coverage rates) as `corpus_summary.json`. For methodology section numbers.

## Files Changed

| File | Changes |
|------|---------|
| `drehem_extract.py` | +`detect_issues()`, +module constants, trimmed docstrings |
| `build_diagnostics.py` | Rewritten as thin import wrapper |
| `run_dashboard.py` | Removed duplicate `detect_issues`, cleaned imports |
| `build_name_dictionary.py` | Removed demo block, unused import, duplicate header |
| `extract_drehem.py` | Quote style, docstring cleanup |
| `validate_sample.py` | Quote style, docstring cleanup |
| `drehem_analysis.py` | +herd composition, +corpus summary, removed unused import, docstrings |
