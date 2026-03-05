# Session Log — 25 February 2026

## ORACC Name Dictionary & Vocabulary Tool

### Goal
Build a name normalization module from the ORACC Ur III administrative corpus, integrate it into `drehem_extract.py`, and create a local search interface for looking up Sumerian names and vocabulary.

---

### What We Built

#### 1. `build_name_dictionary.py` — ORACC Data Pipeline
Downloads and parses the entire **ORACC epsd2/admin/ur3** corpus (80,181 texts, 536 MB zip) to build two JSON databases:

- **`oracc_name_dictionary.json`** (7.7 MB) — 64,586 transliterated name forms mapped to their canonical names, POS tags, and attestation counts.
- **`oracc_glossary.json`** — 2,716 Sumerian vocabulary entries (animals, professions, transaction terms, etc.) with all attested transliteration forms and English meanings.

The script handles SSL certificate issues on macOS and caches the downloaded zip so re-runs are fast.

#### 2. Integration into `drehem_extract.py`
- Added `normalized_name` and `name_type` fields to the `Person` dataclass.
- Added a `normalize_persons()` function that automatically tags each extracted person with their ORACC canonical name (e.g. `ab-ba-sa6-ga` → **Abbasaga**) and POS type (`PN`, `DN`, `GN`, etc.).
- Added `source_normalized`, `receiver_normalized`, `intermediary_normalized`, `commissioner_normalized` columns to the CSV output.
- Graceful fallback: if the dictionary hasn't been built yet, normalization is silently skipped.

#### 3. `name_search.html` — Local Search Tool
A browser-based search interface (like a personal ePSD/Hubur) with:
- **Names tab** — 27,039 deduplicated entries (personal names, deity names, royals, settlements, etc.)
- **Vocabulary tab** — 2,716 Sumerian words with English meanings and attested forms
- Category filters (Animals, Professions, Deities, Geographic, etc.)
- Fuzzy search, variant spellings, attestation counts
- Run with: `python3 -m http.server 8090` → open `localhost:8090/name_search.html`

---

### Key Findings & Learnings

#### Data Source
- All data comes from **ORACC (Open Richly Annotated Cuneiform Corpus)**, lemmatized by professional Assyriologists. Licence: CC0 (public domain).
- POS tags (`PN`, `DN`, etc.) are **expert annotations**, not algorithmic guesses — very high reliability.

#### Theophoric Names
- Names containing `{d}` (divine determinative) can be either **personal names** or **deity names**:
  - `puzur4-{d}en-lil2` → **Puzurenlil** = "Protection of Enlil" → `PN` (a person)
  - `{d}en-lil2` → **Enlil** = the god himself → `DN` (a deity)
- The ORACC annotations correctly distinguish all 9,990 theophoric PNs from 1,434 deity references.

#### Ur-Nigar / Urniŋarak
- The Sumerian-network project's `people.csv` only listed Urniŋarak as a family member reference (`dumu ur-nigar{gar}`), not as a standalone person — despite being a **major** Ur III administrative figure.
- The ORACC dictionary correctly captures Urniŋarak with **4,585 attestations** — one of the most attested names in the entire corpus.

#### Unicode vs ATF
- ORACC uses Unicode special characters (`š`, `ŋ`, `₂`, `₆`) while ATF transliterations use ASCII (`sz`, `j`, `2`, `6`).
- The dictionary stores both forms, so lookups work regardless of notation convention.

---

### Dictionary Statistics

| Category | Unique Forms |
|---|---|
| Personal names (PN) | 52,246 |
| Settlement names (SN) | 5,265 |
| Field names (FN) | 2,376 |
| Deity names (DN) | 1,929 |
| Month names (MN) | 971 |
| Watercourse names (WN) | 663 |
| Agricultural names (AN) | 341 |
| Temple names (TN) | 273 |
| Royal names (RN) | 230 |
| Geographic names (GN) | 191 |
| **Total** | **64,586** |

Glossary: 2,716 vocabulary entries (animals, professions, verbs, nouns)

---

### Files Created / Modified

| File | Action | Description |
|---|---|---|
| `build_name_dictionary.py` | **NEW** | Downloads ORACC corpus, builds name dictionary + glossary |
| `oracc_name_dictionary.json` | **NEW** | 64,586 name→canonical mappings (7.7 MB) |
| `oracc_glossary.json` | **NEW** | 2,716 Sumerian vocabulary entries |
| `name_search.html` | **NEW** | Local browser search tool |
| `drehem_extract.py` | **MODIFIED** | Added ORACC name normalization integration |
| `.oracc_cache/` | **NEW** | Cached ORACC zip download (536 MB) |

---

### How to Re-Run

```bash
# Rebuild dictionary (uses cached download, ~30s)
python3 build_name_dictionary.py

# Run extraction pipeline with auto-normalization
python3 drehem_extract.py

# Open search tool
python3 -m http.server 8090
# → http://localhost:8090/name_search.html
```

---
