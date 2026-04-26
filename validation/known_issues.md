# Known Issues — Drehem Extraction Pipeline

Running log of identified edge cases and extraction bugs.
Add new entries here when you spot a problem. Once fixed, update Status.

**Workflow:**
1. Spot problem → add entry below (or use `python3 validate_issue.py PXXXXXX --add-issue "description"`)
2. Run `python3 validate_issue.py PXXXXXX --show-lines` to confirm
3. Fix `drehem_extract.py` + add to `validation/ground_truth.json`
4. Run `python3 test_extraction_accuracy.py --suite edge --verbose`
5. Mark **Status: Fixed** + fill in commit reference

---

## [2026-04-01] P123484 (OIP 115, 001) — siki/wool after gu4 counted as live oxen
- **ATF**: `2(disz) gu4 siki ge6-[ge6]`
- **Problem**: Extractor parsed `2 gu4` as 2 live oxen. But `siki` immediately after = wool OF the oxen. The tablet is a wool commodity record, not an animal record. `total_animals` was incorrectly reported as > 0.
- **Status**: Fixed — DERIVED_COMMODITY_MARKERS suppression added
- **Expected**: `total_animals=0`, `transaction_type="to accept"`
- **Ground truth entry added**: Yes (id: 123484)

---

## [2026-04-01] P123329 (OIP 115, 003) — mu-[kux(DU)...] not recognized as delivery
- **ATF**: `mu-[kux(DU) ... x]` (line 5, obverse)
- **Problem**: `strip_atf_damage()` removes bracket contents → `mu-[kux...]` becomes `mu-` → no match. Transaction type = "" (empty) instead of "delivery".
- **Status**: Fixed — pre-check raw lines for `mu-\[kux` before damage stripping
- **Expected**: `transaction_type="delivery"`
- **Ground truth entry added**: Yes (id: 123329)

---

## [2026-04-01] P123525 (OIP 115, 028) — ad6/ad7 carcass markers, animals not counted
- **ATF**: `3(disz) ad7 gu4`, `5(u) 8(disz) ad7 udu u2`, `7(disz) ad6? masz2`
- **Problem**: `ad6` and `ad7` are carcass/dead-animal markers that appear *between* the numeral and the animal name. The extractor stopped at `ad7` (unrecognized) and didn't find the animal. `total_animals = 0` instead of 68.
- **Status**: Fixed — CARCASS_MARKERS set added; extractor skips marker and matches trailing animal with qualifier `ba-usz2`
- **Expected**: `total_animals=68`, `transaction_type="expenditure"`, all animals have qualifier `ba-usz2`
- **Ground truth entry added**: Yes (id: 123525)

---

## [2026-04-01] Multiple tablets — floating `#` and `?` in cleaned text / whitespace gaps
- **ATF**: Any line with `ge6-[ge6]`, `ad6?`, etc.
- **Problem**: Old `strip_atf_damage` removed `[...]` contents but left order issues with `<<>>` corrections and didn't collapse double-spaces left by removal.
- **Status**: Fixed — new ordering: `<<>>` first, then `<>`, then `[...]`, then `#?!`, then whitespace collapse
- **Ground truth entry added**: N/A (systemic)

---
<!-- Add new issues below this line -->
