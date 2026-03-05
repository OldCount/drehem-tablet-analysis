#!/usr/bin/env python3
"""
Build a name-normalization dictionary from the ORACC epsd2/admin/ur3 corpus.

Downloads ORACC JSON data, parses lemmatized tokens, and extracts Personal Names
(PN), Geographic Names (GN), Deity Names (DN), Royal Names (RN), and more.
Output: oracc_name_dictionary.json (transliterated form → canonical name + POS).

Data source: ORACC (http://oracc.org/epsd2/u3adm) — Creative Commons public data.
"""

import json
import re
import ssl
import subprocess
import zipfile
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from collections import defaultdict


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# ORACC JSON zip download URLs (primary + fallback mirrors)
ORACC_ZIP_URLS = [
    "http://oracc.museum.upenn.edu/json/epsd2-admin-ur3.zip",
    "http://build-oracc.museum.upenn.edu/json/epsd2-admin-ur3.zip",
]

# Part-of-speech tags we want to capture from ORACC lemmatization
POS_TAGS_OF_INTEREST = {
    "PN":  "personal_name",
    "GN":  "geographic_name",
    "DN":  "deity_name",
    "RN":  "royal_name",
    "SN":  "settlement_name",
    "WN":  "watercourse_name",
    "ON":  "object_name",
    "FN":  "field_name",
    "TN":  "temple_name",
    "MN":  "month_name",
    "QN":  "quarter_name",
    "AN":  "agricultural_name",
    "LN":  "line_name",
}

# Where to cache the downloaded zip and the built dictionary
CACHE_DIR = Path(__file__).parent / ".oracc_cache"
ZIP_CACHE = CACHE_DIR / "epsd2-admin-ur3.zip"
DICT_OUTPUT = Path(__file__).parent / "oracc_name_dictionary.json"
GLOSSARY_OUTPUT = Path(__file__).parent / "oracc_glossary.json"

# Semantic categories we assign to glossary entries based on guide-word
# matching. This helps the search UI filter by topic.
ANIMAL_KEYWORDS = {
    "sheep", "goat", "ox", "cow", "cattle", "lamb", "kid", "bull",
    "donkey", "pig", "bird", "dog", "gazelle", "deer", "horse",
    "ewe", "ram", "he-goat", "she-goat", "calf", "foal", "animal",
    "duck", "goose", "fish", "wild", "piglet", "billy-goat",
    "wild bull", "fledgling", "turtle", "hare", "bear",
}

PROFESSION_KEYWORDS = {
    "scribe", "priest", "priestess", "overseer", "king", "governor",
    "shepherd", "cook", "administrator", "messenger", "official",
    "fattener", "herdsman", "general", "secretary", "cup-bearer",
    "brewer", "butcher", "guard", "merchant", "farmer", "cowherd",
    "gendarme", "seal", "queen", "prince", "princess", "singer",
    "craftsman", "smith", "fuller", "weaver", "potter", "carpenter",
    "leatherworker", "reed-worker", "physician", "diviner",
    "plowman", "gardener", "fisherman", "boatman", "courier",
    "barber", "sweeper",
}


# ---------------------------------------------------------------------------
# Downloading
# ---------------------------------------------------------------------------

def download_oracc_zip(force: bool = False) -> Path:
    """
    Download the ORACC JSON zip archive for the Ur III admin corpus.

    Tries multiple mirror URLs in case one is unavailable. Caches the
    downloaded zip locally so subsequent runs don't need the network.

    Returns the path to the cached zip file.
    """
    if ZIP_CACHE.exists() and not force:
        size_mb = ZIP_CACHE.stat().st_size / (1024 * 1024)
        print(f"  Using cached zip: {ZIP_CACHE} ({size_mb:.1f} MB)")
        return ZIP_CACHE

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    for url in ORACC_ZIP_URLS:
        print(f"  Trying: {url}")

        # Attempt 1: urllib with SSL verification disabled (macOS cert issue)
        try:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            req = Request(url, headers={"User-Agent": "DrehemResearch/1.0"})
            with urlopen(req, timeout=120, context=ctx) as response:
                data = response.read()
                ZIP_CACHE.write_bytes(data)
                size_mb = len(data) / (1024 * 1024)
                print(f"  Downloaded {size_mb:.1f} MB → {ZIP_CACHE}")
                return ZIP_CACHE
        except (URLError, HTTPError, TimeoutError) as e:
            print(f"  urllib failed: {e}")

        # Attempt 2: fall back to curl (handles certs natively on macOS)
        try:
            result = subprocess.run(
                ["curl", "-skL", "-o", str(ZIP_CACHE), url],
                timeout=300, capture_output=True,
            )
            if result.returncode == 0 and ZIP_CACHE.exists() and ZIP_CACHE.stat().st_size > 1000:
                size_mb = ZIP_CACHE.stat().st_size / (1024 * 1024)
                print(f"  Downloaded via curl {size_mb:.1f} MB → {ZIP_CACHE}")
                return ZIP_CACHE
            else:
                print(f"  curl failed: {result.stderr.decode()[:200]}")
        except (FileNotFoundError, subprocess.TimeoutExpired) as e:
            print(f"  curl fallback failed: {e}")

    raise ConnectionError(
        "Could not download ORACC data from any mirror. "
        "Check your internet connection or try again later."
    )


# ---------------------------------------------------------------------------
# JSON parsing — walking the ORACC CDL tree
# ---------------------------------------------------------------------------

def walk_cdl(node: dict | list) -> list[dict]:
    """
    Recursively walk ORACC CDL tree and extract all lemmatized word nodes.
    Each lemma node ("node": "l") contains an "f" dict with form, cf (canonical),
    gw (gloss), pos, and epos fields.
    """
    results = []

    if isinstance(node, list):
        for item in node:
            results.extend(walk_cdl(item))
        return results

    if not isinstance(node, dict):
        return results

    # Lemma node: ORACC uses "node": "l" (not "type": "l")
    if node.get("node") == "l" and "f" in node:
        results.append(node["f"])

    # Recurse into child CDL arrays
    if "cdl" in node:
        results.extend(walk_cdl(node["cdl"]))

    return results


def parse_text_json(data: dict) -> list[dict]:
    """
    Parse a single ORACC text JSON and return all lemmatized form dicts.

    Each text JSON has a top-level "cdl" array containing the full
    hierarchical annotation of one cuneiform tablet.
    """
    if "cdl" not in data:
        return []
    return walk_cdl(data["cdl"])


# ---------------------------------------------------------------------------
# Building the dictionary
# ---------------------------------------------------------------------------

def build_dictionary(zip_path: Path) -> dict:
    """
    Extract name attestations from ORACC zip and build a normalization dictionary.
    For each lemma with pos ∈ {PN, GN, DN, RN, ...}, maps transliterated form to
    canonical name, POS, category, and attestation count. Resolves ambiguities by
    selecting the most frequently attested canonical form.
    """
    # Intermediate: form → { canonical → count }
    form_candidates: dict[str, dict[str, dict]] = defaultdict(
        lambda: defaultdict(lambda: {"count": 0, "pos": "", "category": ""})
    )

    texts_parsed = 0
    tokens_found = 0

    print("  Parsing ORACC JSON files...")

    with zipfile.ZipFile(zip_path, "r") as zf:
        json_files = [
            name for name in zf.namelist()
            if name.endswith(".json") and "corpusjson" in name
        ]

        total = len(json_files)
        for i, name in enumerate(json_files):
            if (i + 1) % 2000 == 0 or i == total - 1:
                print(f"    {i + 1:,} / {total:,} texts processed...")

            try:
                with zf.open(name) as f:
                    data = json.load(f)
            except (json.JSONDecodeError, KeyError):
                continue

            forms = parse_text_json(data)
            texts_parsed += 1

            for form_data in forms:
                pos = form_data.get("pos", "")
                epos = form_data.get("epos", pos)

                # Use effective POS if available (handles disambiguation)
                effective_pos = epos if epos in POS_TAGS_OF_INTEREST else pos

                if effective_pos not in POS_TAGS_OF_INTEREST:
                    continue

                raw_form = form_data.get("form", "").strip()
                canonical = form_data.get("cf", "").strip()

                if not raw_form or not canonical:
                    continue

                # Store both the Unicode form (as ORACC gives it) and the
                # ATF-ASCII form (as drehem_extract.py uses it) so lookup
                # works regardless of which convention the caller uses.
                clean_form = _clean_form(raw_form)
                if not clean_form:
                    continue

                atf_form = _unicode_to_atf(clean_form)

                category = POS_TAGS_OF_INTEREST[effective_pos]

                # Register both the Unicode and ATF forms
                for form_key in {clean_form, atf_form}:
                    entry = form_candidates[form_key][canonical]
                    entry["count"] += 1
                    entry["pos"] = effective_pos
                    entry["category"] = category
                tokens_found += 1

    # Resolve ambiguities: pick the most-attested canonical form
    dictionary = {}
    for form, candidates in form_candidates.items():
        best_canonical = max(candidates.keys(), key=lambda c: candidates[c]["count"])
        best = candidates[best_canonical]
        dictionary[form] = {
            "canonical": best_canonical,
            "pos": best["pos"],
            "category": best["category"],
            "attestations": best["count"],
        }

    print(f"  Parsed {texts_parsed:,} texts, found {tokens_found:,} name tokens")
    print(f"  Built dictionary with {len(dictionary):,} unique forms")

    return dictionary


# Unicode subscripts → ASCII digits, used to convert between ORACC
# Unicode notation (e.g. ša₃, ge₆, sa₆) and ATF ASCII (sza3, ge6, sa6)
_UNICODE_SUBSCRIPTS = str.maketrans("₀₁₂₃₄₅₆₇₈₉", "0123456789")

# ORACC uses Unicode special chars; ATF uses ASCII equivalents.
# Map both directions so lookup works with either convention.
_UNICODE_TO_ATF = {
    "š": "sz",
    "Š": "SZ",
    "ŋ": "j",    # some ATF uses j for eng
    "ṭ": "t,",
    "ṣ": "s,",
    "ʾ": "'",
}


def _unicode_to_atf(form: str) -> str:
    """
    Convert ORACC Unicode transliteration to ATF ASCII notation.

    E.g. "ab-ba-sa₆-ga" → "ab-ba-sa6-ga"
         "šu-{d}šara₂"  → "szu-{d}szara2"
    """
    # Subscript digits → normal digits
    form = form.translate(_UNICODE_SUBSCRIPTS)
    # Special consonants
    for uni, atf in _UNICODE_TO_ATF.items():
        form = form.replace(uni, atf)
    return form


def _clean_form(form: str) -> str:
    """
    Normalize an ORACC transliteration form for use as a lookup key.

    Removes ATF damage markers (#, ?, !, [...]) and Unicode normalization
    artifacts so that lookup works against cleaned ATF text.
    """
    # Remove bracketed reconstructions: [x] → x
    form = re.sub(r"\[([^\]]*)\]", r"\1", form)
    # Remove damage markers
    form = form.replace("#", "").replace("?", "").replace("!", "")
    # Remove angle-bracket corrections: <<x>> → ""
    form = re.sub(r"<<[^>]*>>", "", form)
    # Collapse whitespace
    form = " ".join(form.split())
    return form.strip()


# ---------------------------------------------------------------------------
# Saving and loading
# ---------------------------------------------------------------------------

def save_dictionary(dictionary: dict, output_path: Path) -> None:
    """
    Save the name dictionary to a JSON file, sorted by category then form.
    """
    # Sort: PN first, then GN, DN, etc., then alphabetically within each
    category_order = list(POS_TAGS_OF_INTEREST.values())

    sorted_dict = dict(sorted(
        dictionary.items(),
        key=lambda item: (
            category_order.index(item[1]["category"])
            if item[1]["category"] in category_order else 99,
            item[0],
        ),
    ))

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(sorted_dict, f, ensure_ascii=False, indent=1)

    size_kb = output_path.stat().st_size / 1024
    print(f"  Saved → {output_path} ({size_kb:.0f} KB)")


def load_dictionary(path: Path | None = None) -> dict:
    """
    Load a previously built name dictionary from JSON.

    If no path is given, looks for the default output location.
    Returns an empty dict if the file doesn't exist yet.
    """
    if path is None:
        path = DICT_OUTPUT

    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Name normalization (the function you call from drehem_extract.py)
# ---------------------------------------------------------------------------

# Module-level cache so the dictionary is loaded only once
_LOADED_DICT: dict | None = None


def normalize_name(
    transliterated_name: str,
    dictionary: dict | None = None,
) -> dict | None:
    """
    Look up a transliterated name and return its normalized form.
    Returns {canonical, pos, category, attestations} or None if not found.
    Auto-loads dictionary from disk if not provided.
    """
    global _LOADED_DICT

    if dictionary is None:
        if _LOADED_DICT is None:
            _LOADED_DICT = load_dictionary()
        dictionary = _LOADED_DICT

    if not dictionary:
        return None

    clean = _clean_form(transliterated_name)
    atf = _unicode_to_atf(clean)

    # Try direct lookup (both Unicode and ATF forms)
    for form in (clean, atf):
        if form in dictionary:
            return dictionary[form]

    # Try stripping Sumerian case suffixes (-ta, -sze3, -ra, -ke4, etc.)
    suffixes = ["-ta", "-sze3", "-ra", "-ke4", "-ka", "-kam", "-me", "-e"]
    for form in (clean, atf):
        for suffix in suffixes:
            if form.endswith(suffix):
                stripped = form[:-len(suffix)]
                if stripped in dictionary:
                    return dictionary[stripped]

    return None


def normalize_name_batch(
    names: list[str],
    dictionary: dict | None = None,
) -> list[dict | None]:
    """
    Normalize a list of names in one call (avoids repeated dict loading).

    Returns a list of results, one per input name (None if not found).
    """
    if dictionary is None:
        dictionary = load_dictionary()

    return [normalize_name(name, dictionary) for name in names]


# ---------------------------------------------------------------------------
# Summary statistics
# ---------------------------------------------------------------------------

def print_summary(dictionary: dict) -> None:
    """
    Print a summary of what the dictionary contains.
    """
    by_category = defaultdict(int)
    by_pos = defaultdict(int)
    total_attestations = 0

    for entry in dictionary.values():
        by_category[entry["category"]] += 1
        by_pos[entry["pos"]] += 1
        total_attestations += entry["attestations"]

    print(f"\n{'='*60}")
    print(f"ORACC NAME DICTIONARY SUMMARY")
    print(f"{'='*60}")
    print(f"  Total unique forms:     {len(dictionary):,}")
    print(f"  Total attestations:     {total_attestations:,}")
    print()
    print(f"  By category:")
    for cat in POS_TAGS_OF_INTEREST.values():
        count = by_category.get(cat, 0)
        if count > 0:
            print(f"    {cat:<25} {count:>6,}")

    # Top 20 most-attested personal names
    pn_entries = [
        (form, entry) for form, entry in dictionary.items()
        if entry["pos"] == "PN"
    ]
    pn_entries.sort(key=lambda x: x[1]["attestations"], reverse=True)

    print(f"\n  Top 20 most-attested personal names:")
    print(f"  {'Transliteration':<30} {'Canonical':<25} {'Count':>6}")
    print(f"  {'-'*30} {'-'*25} {'-'*6}")
    for form, entry in pn_entries[:20]:
        print(f"  {form:<30} {entry['canonical']:<25} {entry['attestations']:>6}")

    # Top 10 geographic names
    gn_entries = [
        (form, entry) for form, entry in dictionary.items()
        if entry["pos"] == "GN"
    ]
    gn_entries.sort(key=lambda x: x[1]["attestations"], reverse=True)

    if gn_entries:
        print(f"\n  Top 10 geographic names:")
        print(f"  {'Transliteration':<30} {'Canonical':<25} {'Count':>6}")
        print(f"  {'-'*30} {'-'*25} {'-'*6}")
        for form, entry in gn_entries[:10]:
            print(f"  {form:<30} {entry['canonical']:<25} {entry['attestations']:>6}")


# ---------------------------------------------------------------------------
# Glossary extraction (animals, professions, all Sumerian vocabulary)
# ---------------------------------------------------------------------------

def _classify_glossary_entry(gw: str) -> str:
    """
    Assign a semantic category to a glossary entry based on its
    guide word (English meaning).

    Returns a category string like 'animal', 'profession', 'verb',
    'noun', etc.
    """
    gw_lower = gw.lower()
    if any(kw in gw_lower for kw in ANIMAL_KEYWORDS):
        return "animal"
    if any(kw in gw_lower for kw in PROFESSION_KEYWORDS):
        return "profession"
    return "other"


def build_glossary(zip_path: Path) -> list[dict]:
    """
    Extract Sumerian vocabulary glossary from ORACC zip (gloss-sux.json).
    Parses cf (citation form), gw (guide word), pos (POS), forms, and icount
    (attestation count). Returns a list sorted by frequency for the search tool.
    """
    print("  Extracting Sumerian glossary...")

    with zipfile.ZipFile(zip_path, "r") as zf:
        gloss_path = "epsd2/admin/ur3/gloss-sux.json"
        if gloss_path not in zf.namelist():
            print("  WARNING: gloss-sux.json not found in zip")
            return []

        with zf.open(gloss_path) as f:
            gloss_data = json.load(f)

    entries = gloss_data.get("entries", [])
    result = []

    for entry in entries:
        cf = entry.get("cf", "").strip()
        gw = entry.get("gw", "").strip()
        pos = entry.get("pos", "").strip()
        icount = int(entry.get("icount", 0))

        if not cf or not gw:
            continue

        # Collect all attested forms (transliterations)
        forms = []
        for form_data in entry.get("forms", []):
            form_name = form_data.get("n", "").strip()
            form_count = int(form_data.get("icount", 0))
            if form_name:
                # Convert Unicode to ATF as well
                atf_form = _unicode_to_atf(form_name)
                forms.append({
                    "form": form_name,
                    "atf": atf_form,
                    "count": form_count,
                })

        # Classify into semantic category
        category = _classify_glossary_entry(gw)

        result.append({
            "citation": cf,
            "meaning": gw,
            "pos": pos,
            "category": category,
            "attestations": icount,
            "forms": sorted(forms, key=lambda f: f["count"], reverse=True),
        })

    result.sort(key=lambda e: e["attestations"], reverse=True)

    # Stats
    animals = sum(1 for e in result if e["category"] == "animal")
    profs = sum(1 for e in result if e["category"] == "profession")
    print(f"  Found {len(result):,} vocabulary entries")
    print(f"    Animals:     {animals:,}")
    print(f"    Professions: {profs:,}")
    print(f"    Other:       {len(result) - animals - profs:,}")

    return result


def save_glossary(glossary: list[dict], output_path: Path) -> None:
    """
    Save the glossary to a JSON file.
    """
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(glossary, f, ensure_ascii=False, indent=1)

    size_kb = output_path.stat().st_size / 1024
    print(f"  Saved → {output_path} ({size_kb:.0f} KB)")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Full pipeline: download ORACC data → parse → build dictionary + glossary → save.
    """
    print("Building ORACC name dictionary & glossary...")
    print()

    # Step 1: Download
    print("[1/4] Downloading ORACC corpus...")
    zip_path = download_oracc_zip()

    # Step 2: Parse and build name dictionary
    print("[2/4] Building name dictionary from lemmatized data...")
    dictionary = build_dictionary(zip_path)

    # Step 3: Build Sumerian vocabulary glossary
    print("[3/4] Building Sumerian vocabulary glossary...")
    glossary = build_glossary(zip_path)

    # Step 4: Save
    print("[4/4] Saving...")
    save_dictionary(dictionary, DICT_OUTPUT)
    save_glossary(glossary, GLOSSARY_OUTPUT)

    print_summary(dictionary)


if __name__ == "__main__":
    main()
