#!/usr/bin/env python3
"""Generate annotation data for the tablet visualization tool.

Produces a JSON file mapping each token in a transliteration to its semantic
role as identified by the extraction algorithm. Used by tablet_vis.html.

Usage:
    python tablet_visualizer.py                  # serve on port 8585
    python tablet_visualizer.py --tablet 100214  # export single tablet JSON
"""

import csv
import json
import re
import argparse
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

from drehem_extract import (
    extract_tablet, extract_content_lines, detect_issues,
    strip_atf_damage, parse_numeral, compute_extraction_score,
    ANIMAL_TERMS, MEASURE_TERMS, NON_ANIMAL_LINE_MARKERS,
    NUMERAL_PATTERN, OFFICIALS_TO_OFFICE, MONTH_NAMES,
    _extract_edge_total,
)

BASE_DIR = Path(__file__).parent
DATABASE = BASE_DIR / "drehem_database.csv"
EXTRACTED = BASE_DIR / "drehem_extracted.csv"

# ---------------------------------------------------------------------------
# Sumerian-English-Dutch translation dictionary
# ---------------------------------------------------------------------------

TRANSLATIONS = {
    # Animals
    "udu": ("sheep", "schaap"),
    "masz2": ("goat", "geit"),
    "sila4": ("lamb", "lam"),
    "gu4": ("ox", "os"),
    "ab2": ("cow", "koe"),
    "amar": ("calf", "kalf"),
    "masz-da3": ("gazelle", "gazelle"),
    "masz-da3-nita2": ("male gazelle", "mannelijke gazelle"),
    "u8": ("ewe", "ooi"),
    "ud5": ("nanny goat", "geit"),
    "masz2-gal": ("he-goat", "mannetjesbok"),
    "sza-lim": ("healthy animal", "gezond dier"),
    "udu-nita2": ("ram", "ram"),
    "udu-nita": ("ram", "ram"),
    "gukkal": ("fat-tailed sheep", "vetstaartschaap"),
    "dara3": ("ibex", "steenbok"),
    "dara3-masz": ("ibex", "steenbok"),
    "am": ("wild bull", "wilde stier"),
    "am-si": ("elephant", "olifant"),
    "pirig": ("lion", "leeuw"),
    "pirig-tur": ("young lion", "jonge leeuw"),
    "ur-mah": ("lion", "leeuw"),
    "ur-bar-ra": ("wolf", "wolf"),
    "uz": ("goose", "gans"),
    "kunga2": ("equid", "ezel/paard"),
    "kunga2-nita2": ("male equid", "mannelijke ezel"),
    "|U8+HUL2|": ("sheep (composite)", "schaap (samengesteld)"),
    "kun-gid2": ("fat-tailed sheep", "vetstaartschaap"),
    "dusu2-nita2": ("male donkey", "mannelijke ezel"),
    "dusu2-munus": ("female donkey", "vrouwelijke ezel"),
    "dusu2": ("donkey", "ezel"),
    "dur3": ("young male donkey", "jonge mannelijke ezel"),
    "eme6": ("female donkey", "vrouwelijke ezel"),
    # Birds
    "buru4{muszen}": ("sparrow", "mus"),
    "buru5{muszen}": ("sparrow", "mus"),
    "buru5{muszen}-gi": ("bird", "vogel"),
    "muszen": ("bird", "vogel"),
    "tu-gur4{muszen}": ("dove", "duif"),
    "ir7{muszen}": ("pigeon", "duif"),
    "uz-tur": ("duckling", "eendje"),
    # Qualifiers
    "niga": ("fattened", "vetgemest"),
    "u2": ("grass-fed", "grasgevoerd"),
    "a-lum": ("breeding male", "fokdier"),
    "bar-gal": ("large breed", "groot ras"),
    "bar-su-ga": ("fleece restored", "vacht hersteld"),
    "nita2": ("male", "mannelijk"),
    "nita": ("male", "mannelijk"),
    "munus": ("female", "vrouwelijk"),
    "gal": ("large", "groot"),
    "tur": ("small/young", "klein/jong"),
    "ba-usz2": ("dead", "dood"),
    "ba-ug7": ("dead", "dood"),
    "usz2": ("dead", "dood"),
    "saga": ("fine/good quality", "fijn/goede kwaliteit"),
    # Transaction types
    "mu-kux(du)": ("delivery (incoming)", "levering (inkomend)"),
    "mu-kux": ("delivery (incoming)", "levering (inkomend)"),
    "ba-zi": ("expenditure (outgoing)", "uitgave (uitgaand)"),
    "i3-dab5": ("transfer (took over)", "overdracht (overgenomen)"),
    "szu": ("hand", "hand"),
    "ba-ti": ("received", "ontvangen"),
    "szu ba-ti": ("to accept / to receive", "accepteren / ontvangen"),
    "zi-ga": ("expenditure (outgoing)", "uitgave (uitgaand)"),
    "lulim-munus": ("female red deer/stag", "vrouwelijk edelhert/hinde"),
    "diri": ("extra / surplus", "extra / overschot"),
    "nig2-diri": ("extra / surplus", "extra / overschot"),
    "e2-u4-1(u)-5(disz)": ("house of the 15th day (full moon)", "huis van de 15e dag (volle maan)"),
    "e2 u4-sakar": ("house of the crescent moon", "huis van de wassende maan"),
    "e2-u4-sakar": ("house of the crescent moon", "huis van de wassende maan"),
    # Role markers
    "ki": ("place / from", "plaats / van"),
    "giri3": ("via (intermediary)", "via (tussenpersoon)"),
    "maszkim": ("commissioner", "commissaris"),
    "kiszib3": ("seal / sealed by", "zegel / verzegeld door"),
    # Structural
    "szu-nigin": ("grand total", "eindtotaal"),
    "szunigin": ("grand total", "eindtotaal"),
    "sza3-bi-ta": ("from among them", "daarvan"),
    "sa2-du11": ("regular offering", "reguliere offergave"),
    "nig2-dab5": ("provisions", "provisie"),
    "iti": ("month", "maand"),
    "mu": ("year / for (name)", "jaar / voor (naam)"),
    "u4": ("day", "dag"),
    "-kam": ("(ordinal)", "(rangtelwoord)"),
    "-ta": ("from", "van"),
    "-sze3": ("to / for", "naar / voor"),
    # Measures / non-animal
    "barig": ("volume (barig)", "volume (barig)"),
    "ban2": ("volume (ban)", "volume (ban)"),
    "sila3": ("volume (sila)", "volume (sila)"),
    "gin2": ("weight (shekel)", "gewicht (sikkel)"),
    "gur": ("volume (gur)", "volume (gur)"),
    "sze": ("barley", "gerst"),
    "zid2": ("flour", "meel"),
    "zi3": ("flour", "meel"),
    "dabin": ("semolina", "griesmeel"),
    "kasz": ("beer", "bier"),
    "ma-na": ("mina (weight)", "mina (gewicht)"),
    "gu2": ("talent (weight)", "talent (gewicht)"),
    "ku3-babbar": ("silver", "zilver"),
    "ku3-sig17": ("gold", "goud"),
    "tug2": ("textile", "textiel"),
    "siki": ("wool", "wol"),
    "sa": ("bundle", "bundel"),
    "gi": ("reed", "riet"),
    "ninda": ("bread", "brood"),
    "gurusz": ("worker (male)", "arbeider (man)"),
    "geme2": ("worker (female)", "arbeidster (vrouw)"),
    "erin2": ("troops/workers", "troepen/arbeiders"),
    "dumu": ("child/son", "kind/zoon"),
    # Titles
    "dub-sar": ("scribe", "schrijver"),
    "sanga": ("temple administrator", "tempeladministrateur"),
    "muhaldim": ("cook", "kok"),
    "ra2-gaba": ("rider/messenger", "ruiter/boodschapper"),
    "kuruszda": ("fattener", "vetmester"),
    "nu-banda3": ("overseer", "opzichter"),
    "ugula": ("foreman", "voorman"),
    "szagina": ("general", "generaal"),
    "sza3-tam": ("administrator", "beheerder"),
    "sipa": ("shepherd", "herder"),
    "unu3": ("cowherd", "koeherder"),
    "nagar": ("carpenter", "timmerman"),
    "aga3-us2": ("soldier", "soldaat"),
    "lu2-kin-gi4-a": ("messenger", "boodschapper"),
    "deliverer": ("deliverer", "leveraar"),
    "commissioner": ("commissioner", "commissaris"),
    "receiver": ("receiver", "ontvanger"),
    "intermediary": ("intermediary", "tussenpersoon"),
    "sealer": ("sealer", "bezegelaar"),
    "source": ("source", "bron"),
    # Common non-animal terms
    "ensi2": ("governor", "gouverneur"),
    "sukkal": ("minister", "minister"),
    "szagina": ("general", "generaal"),
    "lugal": ("king", "koning"),
    "e2": ("house/temple", "geen vertaling"),
    "dingir": ("god", "god"),
    "en": ("lord/priest", "heer/priester"),
    "a-ba": ("father", "vader"),
    "ama": ("mother", "moeder"),
    "szesz": ("brother", "broer"),
    "balag": ("harp/drum", "harp/trommel"),
    "ma2": ("boat", "boot"),
    "siskur2": ("offering/ritual", "ritueel/offergave"),
    "a-sza3-ga": ("field/meadow", "veld/weide"),
    "nu2-a": ("lying down/mating", "liggend/parend"),
    "kaskal": ("journey/expedition", "reis/expeditie"),
    "sza3": ("in/at (location)", "in/bij (locatie)"),
    "bala": ("bala-tax/rotating obligation", "bala-belasting/roterende verplichting"),
    "u3-tu-da": ("offspring/newborn", "nageslacht/pasgeboren"),
    "e2-udu": ("sheephouse", "schapenhuis"),
    "e2-udu-sag": ("sheephouse", "schapenhuis"),
    "e2-udu-ka": ("sheephouse", "schapenhuis"),
    "tummal": ("Tummal (sacred site)", "Tummal (heilige plaats)"),
    "la2": ("minus", "min"),
    "dub-la2-mah": ("Dublamah (great binding place)", "Dublamah (grote bindplaats)"),
    "e2-uz-ga": ("royal warehouse", "koninklijk magazijn"),
    "uzu": ("meat", "vlees"),
    "uzu-a": ("cooked meat", "gekookt vlees"),
    "ka-izi": ("roasting (mouth of fire)", "roosteren (mond van vuur)"),
    "ka-izi-sze3": ("for roasting", "om te roosteren"),
}

# Dutch month translations
MONTH_TRANSLATIONS_NL = {
    1: "maand 1", 2: "maand 2", 3: "maand 3", 4: "maand 4",
    5: "maand 5", 6: "maand 6", 7: "maand 7", 8: "maand 8",
    9: "maand 9", 10: "maand 10", 11: "maand 11", 12: "maand 12",
}

TRANSACTION_KEYWORDS = {
    "mu-kux(du)": "delivery",
    "mu-kux(du)-ra-ta": "delivery",
    "mu-kux(du)-ra": "delivery",
    "mu-kux": "delivery",
    "ba-zi": "expenditure",
    "i3-dab5": "transfer",
    "szu ba-ti": "to accept",
    "zi-ga": "expenditure",
}

ROLE_KEYWORDS = {
    "ki": "source_marker",
    "-ta": "source_suffix",
    "giri3": "intermediary_marker",
    "maszkim": "commissioner_marker",
    "kiszib3": "sealer_marker",
    "sza3": "location_marker",
    "-sze3": "destination_suffix",
}

STRUCTURAL_KEYWORDS = {
    "szu-nigin": "summary",
    "szunigin": "summary",
    "sza3-bi-ta": "subsection_marker",
    "sa2-du11": "regular_offering",
    "nig2-dab5": "provisions",
    "iti": "month_marker",
    "u4": "day_marker",
    "u3-tu-da": "offspring/newborn",
    "bala": "bala-tax",
}
# Note: "mu" removed — it's only structural at the START of year-name lines.
# In other positions it means "for" (dative) or is part of person names.

TEMPLE_NAMES = {
    "dub-la2-mah": "Dublamah (great binding place)",
}

DESTINATION_TERMS = {
    "e2-muhaldim": "kitchen",
    "e2-kiszib3-ba": "warehouse",
    "e2-gal": "palace",
    "e2-gal-la": "palace",
    "e2-uz-ga": "royal warehouse",
    "e2-udu": "sheephouse",
    "e2-udu-sag": "sheephouse",
    "e2-udu-ka": "sheephouse",
    "e2-udu-niga": "fattening-house",
    "aga3-us2-e-ne": "soldiers",
    "kas4-ke4-ne": "runners",
    "du6-ku3": "sacred-site",
    "du6-ku3-ga": "sacred-site",
    "e2-u4-1(u)-5(disz)": "15th-day-house",
    "e2 u4-sakar": "crescent-moon-house",
    "e2-u4-sakar": "crescent-moon-house",
}

DEITY_PREFIX = "{d}"
ALL_OFFICIALS = set(k.lower() for k in OFFICIALS_TO_OFFICE)

QUALIFIERS = {
    "niga": "fattened", "u2": "grass-fed", "a-lum": "breeding male",
    "bar-gal": "large breed", "bar-su-ga": "fleece restored",
    "nita2": "male", "munus": "female", "nita": "male",
    "masz2": "billy goat", "sila4": "lamb", "gal": "large",
    "tur": "small/young", "amar": "calf/young",
    "ba-usz2": "dead", "ba-ug7": "dead", "usz2": "dead",
    "saga": "fine/good quality",
}

# Role explanation patterns for the "why" drill-down
ROLE_EXPLANATIONS = {
    "source": "ki {name}-ta → 'from the place of {name}'",
    "receiver": "{transaction_keyword} → '{name} took over / received'",
    "intermediary": "giri3 {name} → 'via {name}'",
    "commissioner": "maszkim {name} → 'commissioner: {name}'",
    "sealer": "kiszib3 {name} → 'sealed by {name}'",
}


def get_translation(token_clean, role):
    """Look up English and Dutch translations for a token."""
    en, nl = "", ""
    if token_clean in TRANSLATIONS:
        en, nl = TRANSLATIONS[token_clean]
    elif role == "numeral":
        val = parse_numeral(token_clean)
        en, nl = str(val), str(val)
    elif role == "deity":
        # Strip {d} prefix for deity name
        name = token_clean.replace("{d}", "").replace("{", "").replace("}", "")
        en = f"god {name}"
        nl = f"god {name}"
    elif role == "damage":
        en, nl = "damaged/illegible", "beschadigd/onleesbaar"
    return en, nl


def _normalize_mukux(token: str) -> str:
    """Normalize mu-kux variants to canonical lowercase form."""
    token = token.replace("ku\u2093", "kux")
    return re.sub(r"mu-kux?\(du\)", "mu-kux(du)", token, flags=re.IGNORECASE)


def _matches_transaction_keyword(token_clean: str, tokens: list[str], idx: int) -> bool:
    """Return True if token at idx starts a transaction keyword match.

    Uses exact token equality (not substring), which prevents false positives
    such as 'ba-ti' inside 'ba-ba-ti' matching the second word of 'szu ba-ti'.
    """
    from drehem_extract import strip_atf_damage as _sad
    tc = _normalize_mukux(token_clean)
    for kw in TRANSACTION_KEYWORDS:
        parts = kw.split()
        first = parts[0]
        if tc == first or token_clean == first:
            if len(parts) == 1:
                return True
            match = True
            for j, part in enumerate(parts[1:], start=1):
                if idx + j >= len(tokens):
                    match = False
                    break
                if _sad(tokens[idx + j]).lower() != part:
                    match = False
                    break
            if match:
                return True
    return False


def _get_transaction_keyword(token_clean: str, tokens: list[str], idx: int) -> str | None:
    """Return the matched transaction keyword string, or None."""
    from drehem_extract import strip_atf_damage as _sad
    tc = _normalize_mukux(token_clean)
    for kw in TRANSACTION_KEYWORDS:
        parts = kw.split()
        first = parts[0]
        if tc != first and token_clean != first:
            continue
        if len(parts) == 1:
            return kw
        match = True
        for j, part in enumerate(parts[1:], start=1):
            if idx + j >= len(tokens) or _sad(tokens[idx + j]).lower() != part:
                match = False
                break
        if match:
            return kw
    return None


_oracc_dict = None

def _load_oracc_dict():
    global _oracc_dict
    if _oracc_dict is not None:
        return _oracc_dict
    path = BASE_DIR / "oracc_name_dictionary.json"
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
            _oracc_dict = {k.replace("ku\u2093", "kux"): v for k, v in raw.items()}
    else:
        _oracc_dict = {}
    return _oracc_dict


def _oracc_lookup(token: str) -> dict | None:
    """Check if token is a known name in the ORACC dictionary."""
    d = _load_oracc_dict()
    if not d:
        return None
    # Try exact match first
    if token in d:
        return d[token]
    # Try stripping case suffixes
    for suf in ("-ta", "-sze3", "-ra", "-ke4", "-ka", "-kam", "-me"):
        if token.endswith(suf) and token[:-len(suf)] in d:
            return d[token[:-len(suf)]]
    return None


_oracc_glossary = None

def _load_oracc_glossary():
    global _oracc_glossary
    if _oracc_glossary is not None:
        return _oracc_glossary
    path = BASE_DIR / "oracc_glossary.json"
    _oracc_glossary = {}
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            glossary_data = json.load(f)
            # Flatten the forms so any text token maps directly to its parent meaning/pos
            for entry in glossary_data:
                meaning = entry.get("meaning", "")
                pos = entry.get("pos", "")
                for form in entry.get("forms", []):
                    atf = form.get("atf", "").lower().replace("ku\u2093", "kux")
                    if atf and atf not in _oracc_glossary:
                        _oracc_glossary[atf] = {"meaning": meaning, "pos": pos}
    return _oracc_glossary


def annotate_tablet(tablet_id, transliteration, date_of_origin=""):
    """Produce token-level annotations for visualization."""
    result = extract_tablet(tablet_id, transliteration, date_of_origin)
    issues = detect_issues(tablet_id, transliteration, result)
    confidence = compute_extraction_score(result, True)
    edge_total = _extract_edge_total(transliteration)

    # Build person name lookup
    person_names = set()
    person_roles = {}
    for p in result.persons:
        n = p.name.lower().strip()
        person_names.add(n)
        r_en, r_nl = get_translation(p.role, "text")
        role_en = r_en or p.role
        role_nl = r_nl or r_en or p.role
        
        detail_en = role_en
        detail_nl = role_nl
        if p.title:
            t_en, t_nl = get_translation(p.title, "text")
            detail_en += f" ({t_en or p.title})"
            detail_nl += f" ({t_nl or t_en or p.title})"
        
        person_roles[n] = {"en": detail_en, "nl": detail_nl}

    # Build animal line index: exact match on cleaned raw text → animal indices
    from collections import defaultdict
    animal_line_map = defaultdict(list)
    for idx, a in enumerate(result.animals):
        raw_clean = strip_atf_damage(a.raw).strip().lower()
        animal_line_map[raw_clean].append(idx)

    annotated_lines = []
    content_line_idx = 0
    current_section = "obverse"
    in_seal = False

    for raw_line in transliteration.split("\n"):
        line = raw_line.strip()
        if not line:
            continue

        # Track sections
        if line.startswith("@"):
            section_lower = line.lower()
            if "obverse" in section_lower:
                current_section = "obverse"
            elif "reverse" in section_lower:
                current_section = "reverse"
            elif "left" in section_lower:
                current_section = "left_edge"
            elif "seal" in section_lower:
                current_section = "seal"
                in_seal = True
            elif in_seal:
                in_seal = False
                current_section = section_lower.replace("@", "")
            if line.startswith("@") and not re.match(r"^\d", line):
                annotated_lines.append({
                    "type": "structural", "section": current_section,
                    "raw": line,
                    "tokens": [{"text": line, "role": "section_header", "en": "", "nl": ""}],
                })
                continue

        if line.startswith(("#", "&")):
            annotated_lines.append({
                "type": "metadata", "section": current_section,
                "raw": line,
                "tokens": [{"text": line, "role": "metadata", "en": "", "nl": ""}],
            })
            continue

        if line.startswith("$"):
            role = "damage_note" if any(k in line.lower() for k in ["broken", "missing", "traces"]) else "ruling"
            annotated_lines.append({
                "type": "structural", "section": current_section,
                "raw": line,
                "tokens": [{"text": line, "role": role, "en": "broken/missing" if role == "damage_note" else "", "nl": "gebroken/ontbrekend" if role == "damage_note" else ""}],
            })
            continue

        # Content lines
        if not re.match(r"^\d+[\.']", line) and not re.match(r"^\.\s+", line):
            continue

        line_num = ""
        content = line
        m = re.match(r"^(\d+[\.'])\s*", line)
        if m:
            line_num = m.group(1)
            content = line[m.end():]
        elif re.match(r"^\.\s+", line):
            content = re.sub(r"^\.\s+", "", line)

        cleaned = strip_atf_damage(content)
        cleaned_lower = cleaned.lower()
        tokens_raw = content.split()
        annotated_tokens = []

        # Determine what extraction this line contributes to
        line_links = []  # list of {"target": "animals.0", ...} extraction links

        # Check if this line matches an animal entry (exact match only)
        if cleaned_lower in animal_line_map:
            for aidx in animal_line_map[cleaned_lower]:
                line_links.append({"target": "animals", "index": aidx})

        # Check if this line contains a person
        for pn in person_names:
            if pn in cleaned_lower:
                line_links.append({"target": "persons", "name": pn, "role": person_roles.get(pn, "")})

        # Check if this line has transaction keyword
        for tk in TRANSACTION_KEYWORDS:
            if tk in cleaned_lower:
                line_links.append({"target": "transaction", "value": TRANSACTION_KEYWORDS[tk]})

        if line_num:
            annotated_tokens.append({"text": line_num, "role": "line_number", "en": "", "nl": ""})

        i = 0
        _prev_tx_continuation = None  # track multi-token keyword continuation
        while i < len(tokens_raw):
            token = tokens_raw[i]
            token_clean = strip_atf_damage(token).lower()
            role = "unknown"
            detail = ""

            # Explicitly separate -ta and -sze3 suffixes so they render alongside structural lines correctly
            suf = ""
            if token_clean.endswith("-ta") and token_clean != "-ta": suf = "-ta"
            elif token_clean.endswith("-sze3") and token_clean != "-sze3": suf = "-sze3"
            
            if suf and _normalize_mukux(token_clean) not in TRANSACTION_KEYWORDS:
                prefix = token_clean[:-len(suf)]
                if (prefix in person_names or prefix in ALL_OFFICIALS or prefix in STRUCTURAL_KEYWORDS or
                    prefix in DESTINATION_TERMS or _oracc_lookup(prefix) or len(prefix) > 3):
                    # Find the suffix split point in the *cleaned* token, then
                    # produce clean prefix and suffix fragments (stripping any
                    # leftover damage markers like square brackets from either side).
                    last_hyphen = token.rfind("-")
                    if last_hyphen != -1:
                        raw_prefix = token[:last_hyphen]
                        raw_suffix = token[last_hyphen:]
                        # Strip any dangling brackets from each fragment
                        # e.g. "puzur4-{d}en-[lil2" → "puzur4-{d}en-[lil2]"
                        # e.g. "-ta]" → "-ta"
                        # But we want to preserve the brackets in the prefix for display,
                        # just close any dangling open bracket so it doesn't look broken.
                        # Actually: we remove stray ] from suffix and stray [ from prefix end
                        raw_suffix = raw_suffix.rstrip("]")
                        # If the prefix ends with an unclosed '[', close it for display
                        if raw_prefix.count("[") > raw_prefix.count("]"):
                            raw_prefix = raw_prefix + "]"
                        tokens_raw[i] = raw_prefix
                        tokens_raw.insert(i + 1, raw_suffix)
                        token = tokens_raw[i]
                        token_clean = strip_atf_damage(token).lower()

            # Check if this token is part of a multi-token transaction keyword
            if _prev_tx_continuation and token_clean == _prev_tx_continuation[0]:
                role = "transaction"
                detail = _prev_tx_continuation[1]
                _prev_tx_continuation = None
                en, nl = get_translation(token_clean, role)
                if not en and detail:
                    en = detail
                annotated_tokens.append({
                    "text": token, "role": role, "detail": detail,
                    "en": en, "nl": nl,
                })
                i += 1
                continue
            _prev_tx_continuation = None

            if NUMERAL_PATTERN.match(token_clean):
                val = parse_numeral(token_clean)
                role = "numeral"
                detail = str(val)

            elif token_clean == "la2":
                role = "numeral"
                detail = "minus"

            elif token_clean in ANIMAL_TERMS:
                role = "animal"
                detail = ANIMAL_TERMS[token_clean]

            elif token_clean in QUALIFIERS:
                role = "qualifier"
                detail = QUALIFIERS[token_clean]

            elif _matches_transaction_keyword(_normalize_mukux(token_clean), tokens_raw, i):
                matched_tx = _get_transaction_keyword(_normalize_mukux(token_clean), tokens_raw, i)
                if matched_tx:
                    role = "transaction"
                    detail = TRANSACTION_KEYWORDS[matched_tx]
                    parts = matched_tx.split()
                    if len(parts) > 1:
                        _prev_tx_continuation = (parts[1], detail)
                else:
                    role = "text"

            elif token_clean in STRUCTURAL_KEYWORDS:
                role = "structural_keyword"
                detail = STRUCTURAL_KEYWORDS[token_clean]

            elif token_clean == "mu" and i == 0:
                # "mu" is only a year marker at line start in year-name lines.
                # Year names: "mu ma2-dara3-abzu...", "mu {d}szu-{d}suen...", "mu us2-sa ..."
                # NOT: "mu aga3-us2-e-ne-sze3" (= for the soldiers, dative)
                # Heuristic: year-name lines are long or contain known patterns
                rest = cleaned_lower[3:] if len(cleaned_lower) > 3 else ""
                is_year_line = (
                    rest.startswith("us2-sa") or
                    rest.startswith("ma2-") or
                    rest.startswith("{d}") or
                    rest.startswith("en-") or
                    rest.startswith("si-") or
                    rest.startswith("ha-") or
                    rest.startswith("a-ra2") or
                    "-ki " in rest or
                    "ba-hul" in rest or
                    "ba-ab-du8" in rest or
                    len(cleaned_lower.split()) >= 5
                )
                if is_year_line:
                    role = "structural_keyword"
                    detail = "year_marker"
                else:
                    role = "text"

            elif token_clean in MONTH_NAMES:
                role = "month"
                detail = f"month {MONTH_NAMES[token_clean]}"

            elif token_clean in TEMPLE_NAMES:
                role = "structural_keyword"
                detail = f"temple: {TEMPLE_NAMES[token_clean]}"

            elif token_clean in person_names and token_clean not in TEMPLE_NAMES:
                role = "person"
                detail = person_roles.get(token_clean, {}).get("en", "")
            elif any(token_clean.startswith(pn + "-") or token_clean.startswith(pn) for pn in person_names if len(pn) > 3 and token_clean.startswith(pn)):
                matched_pn = next(pn for pn in person_names if token_clean.startswith(pn) and len(pn) > 3)
                role = "person"
                detail = person_roles.get(matched_pn, {}).get("en", "") + f" ({matched_pn})"
            elif any(pn in cleaned_lower and token_clean in pn.split("-") for pn in person_names):
                # If token is part of a multi-part person name that exists on this line, tag it as person_part
                matched_pn = next(pn for pn in person_names if pn in cleaned_lower and token_clean in pn.split("-"))
                role = "person_part"
                detail = f"part of {matched_pn}"

            elif token_clean in ALL_OFFICIALS:
                role = "person"
                code, label = OFFICIALS_TO_OFFICE[token_clean]
                detail = f"official ({label})"
            elif any(token_clean.startswith(o + "-") for o in ALL_OFFICIALS if len(o) > 3 and token_clean.startswith(o)):
                matched_o = next(o for o in ALL_OFFICIALS if token_clean.startswith(o) and len(o) > 3)
                code, label = OFFICIALS_TO_OFFICE[matched_o]
                role = "person"
                detail = f"official ({label}, {matched_o})"


            elif token_clean in STRUCTURAL_KEYWORDS:
                role = "structural_keyword"
                detail = STRUCTURAL_KEYWORDS[token_clean]

            elif DEITY_PREFIX in token:
                # First check if it's actually a known personal name that happens to be theophoric
                oracc = _oracc_lookup(token_clean)
                if oracc and oracc.get("pos") == "PN":
                    role = "person"
                    detail = oracc.get("canonical", token_clean)
                elif cleaned_lower.startswith("mu ") and any(
                    t.get("role") == "structural_keyword" and t.get("detail") == "year_marker"
                    for t in annotated_tokens
                ):
                    role = "year_text"
                    detail = "year name element"
                else:
                    role = "deity"
                    detail = "divine name"

            elif token_clean in MEASURE_TERMS:
                if token_clean == "ga" and any(a.animal in cleaned_lower for a in result.animals):
                    # "ga" on an animal line is a qualifier (suckling), not a measure (dairy)
                    role = "qualifier"
                    detail = "suckling"
                else:
                    role = "measure"
                    detail = "non-animal commodity"

            elif token_clean in ("ki", "giri3", "maszkim", "kiszib3", "sza3", "-ta", "-sze3"):
                role = "role_marker"
                detail = ROLE_KEYWORDS.get(token_clean, "")

            elif token_clean in ("dub-sar", "sukkal", "sanga", "muhaldim",
                                 "ra2-gaba", "kuruszda", "ensi2", "nu-banda3",
                                 "ugula", "szagina", "szar2-ra-ab-du",
                                 "sza3-tam", "sipa", "unu3", "nagar",
                                 "aga3-us2", "lu2-kin-gi4-a"):
                role = "title"
                detail = token_clean

            elif "szu-nigin" in token_clean or "szunigin" in token_clean:
                role = "summary_marker"
                detail = "grand total"

            elif token_clean in DESTINATION_TERMS:
                role = "structural_keyword"
                detail = f"destination: {DESTINATION_TERMS[token_clean]}"

            elif "{ki}" in token_clean:
                role = "structural_keyword"
                city = token_clean.replace("{ki}", "")
                detail = f"place: {city}"

            elif token in ("x", "...") or token.startswith("[") or token.endswith("]") or token == "[...]":
                role = "damage"
                detail = "broken/illegible"

            if role == "unknown":
                # ORACC dictionary fallback: recognize personal/geographic/divine names
                oracc = _oracc_lookup(token_clean)
                if oracc and oracc.get("pos") == "PN":
                    role = "person"
                    detail = oracc.get("canonical", token_clean)
                elif oracc and oracc.get("pos") == "DN":
                    role = "deity"
                    detail = oracc.get("canonical", token_clean)
                elif oracc and oracc.get("pos") in ("GN", "SN"):
                    role = "structural_keyword"
                    detail = f"place: {oracc.get('canonical', token_clean)}"
                else:
                    glossary_lookup = _load_oracc_glossary()
                    if token_clean in glossary_lookup:
                        entry = glossary_lookup[token_clean]
                        role = "glossary_term"
                        detail = entry.get("meaning", "known term")
                    else:
                        role = "text"

            en, nl = get_translation(token_clean, role)
            
            # Special override for persons to include their translated roles
            if role == "person":
                for pn in person_names:
                    if pn in token_clean:
                        pr = person_roles.get(pn, {})
                        if pr:
                            if "part of" in detail:
                                en = f"part of {pn} ({pr.get('en', '')})"
                                nl = f"deel van {pn} ({pr.get('nl', '')})"
                            else:
                                en = pr.get("en", en)
                                nl = pr.get("nl", nl)
                        break

            # For roles with detail, use detail as English if no dict entry
            if not en and detail:
                en = detail

            # Detect ATF damage markers on the raw token
            token_damaged = bool(
                "[" in token or "]" in token or "#" in token or "?" in token
            )

            annotated_tokens.append({
                "text": token,
                "role": role,
                "detail": detail,
                "en": en,
                "nl": nl,
                "damaged": token_damaged,
            })
            i += 1

        line_type = "content"
        if any(k in cleaned_lower for k in ("szu-nigin", "szunigin")):
            line_type = "summary"
        elif "iti " in cleaned_lower:
            line_type = "date"
        elif cleaned_lower.startswith("mu ") or cleaned_lower.startswith("mu-"):
            # Only mark as year if it's an actual year-name line (check if mu token was tagged structural)
            if any(t.get("role") == "structural_keyword" and t.get("detail") == "year_marker" for t in annotated_tokens):
                line_type = "year"
        elif any(k in cleaned_lower for k in TRANSACTION_KEYWORDS):
            line_type = "transaction"
        elif any(k in cleaned_lower for k in ("ki ", "giri3 ", "maszkim ", "kiszib3 ")):
            line_type = "person_role"

        annotated_lines.append({
            "type": line_type,
            "section": current_section,
            "raw": line,
            "tokens": annotated_tokens,
            "links": line_links,
            "line_idx": content_line_idx,
        })
        content_line_idx += 1

    # Build extraction summary
    animals_summary = []
    for a in result.animals:
        # Look up English + Dutch translation for the animal
        animal_lower = a.animal.lower().strip()
        anim_en, anim_nl = "", ""
        if animal_lower in TRANSLATIONS:
            anim_en, anim_nl = TRANSLATIONS[animal_lower]
        animals_summary.append({
            "count": a.count,
            "animal": a.animal,
            "qualifiers": a.qualifiers,
            "raw": a.raw,
            "damaged": a.damaged,
            "en": anim_en,
            "nl": anim_nl,
        })

    persons_summary = []
    for p in result.persons:
        office = ""
        bureau_code = ""
        pn = p.name.strip().lower()
        if pn in OFFICIALS_TO_OFFICE:
            bureau_code, office = OFFICIALS_TO_OFFICE[pn]
        elif p.normalized_name:
            # Try normalized name variants (e.g., du11-ga → du-ga)
            nn = p.normalized_name.strip().lower()
            for key in OFFICIALS_TO_OFFICE:
                if nn == key.replace("-", ""):
                    bureau_code, office = OFFICIALS_TO_OFFICE[key]
                    break
        # Build explanation of why this role was assigned
        explanation = ROLE_EXPLANATIONS.get(p.role, "").replace("{name}", p.name)
        if p.role == "receiver":
            explanation = explanation.replace("{transaction_keyword}", result.transaction_type or "?")
        persons_summary.append({
            "name": p.name,
            "role": p.role,
            "title": p.title,
            "normalized": p.normalized_name,
            "office": office,
            "bureau": bureau_code,
            "explanation": explanation,
        })

    divine_recips = []
    for d in result.divine_recipients:
        d_clean = d.replace("{d}", "").replace("{", "").replace("}", "")
        en, nl = get_translation(d_clean, "deity")
        divine_recips.append({
            "name": d,
            "en": en or f"god {d_clean}",
            "nl": nl or f"god {d_clean}",
        })
        
    summary = _build_plain_summary(result, animals_summary, persons_summary,
                                     divine_recips, edge_total)

    return {
        "tablet_id": tablet_id,
        "lines": annotated_lines,
        "extraction": {
            "transaction_type": result.transaction_type,
            "total_animals": result.total_animals,
            "edge_total": edge_total,
            "animals": animals_summary,
            "persons": persons_summary,
            "month": result.month,
            "month_number": getattr(result, 'month_number', None),
            "day": result.day,
            "year": result.year,
            "has_summary_line": result.has_summary_line,
            "confidence": confidence,
            "preservation": result.damage.preservation if result.damage else None,
            "divine_recipients": result.divine_recipients,
            "divine_recipients_translated": divine_recips,
            "destination": result.destination,
        },
        "summary": summary,
        "issues": issues,
    }


def _build_plain_summary(result, animals_summary, persons_summary,
                         divine_recips, edge_total):
    """Generate a plain-language English + Dutch summary of the tablet."""
    tx = result.transaction_type or "unknown transaction"
    tx_map = {
        "delivery": "records a delivery of",
        "expenditure": "records an expenditure of",
        "to accept": "records the acceptance of",
        "transfer": "records a transfer of",
        "royal_delivery": "records a royal delivery of",
        "sub_disbursement": "records a sub-disbursement of",
        "regular_offering": "records regular offerings of",
        "birth_record": "records the birth of",
    }
    tx_map_nl = {
        "delivery": "beschrijft een levering van",
        "expenditure": "beschrijft een uitgave van",
        "to accept": "beschrijft de acceptatie van",
        "transfer": "beschrijft een overdracht van",
        "royal_delivery": "beschrijft een koninklijke levering van",
        "sub_disbursement": "beschrijft een sub-uitgifte van",
        "regular_offering": "beschrijft reguliere offergaven van",
        "birth_record": "beschrijft de geboorte van",
    }
    tx_en = tx_map.get(tx, f"records a {tx} involving")
    tx_nl = tx_map_nl.get(tx, f"beschrijft een {tx} van")

    # Animal description
    animal_parts_en = []
    animal_parts_nl = []
    for a in animals_summary:
        name_en = a.get("en") or a["animal"]
        name_nl = a.get("nl") or a["animal"]
        q = ", ".join(a.get("qualifiers", []))
        animal_parts_en.append(f"{a['count']} {name_en}" + (f" ({q})" if q else ""))
        animal_parts_nl.append(f"{a['count']} {name_nl}" + (f" ({q})" if q else ""))

    if animal_parts_en:
        animals_en = ", ".join(animal_parts_en[:5])
        animals_nl = ", ".join(animal_parts_nl[:5])
        if len(animal_parts_en) > 5:
            animals_en += f" and {len(animal_parts_en) - 5} more entries"
            animals_nl += f" en {len(animal_parts_nl) - 5} meer"
    else:
        animals_en = "animals (details unclear)"
        animals_nl = "dieren (details onduidelijk)"

    # Persons
    source = next((p for p in persons_summary if p["role"] == "source"), None)
    receiver = next((p for p in persons_summary if p["role"] == "receiver"), None)
    intermediary = next((p for p in persons_summary if p["role"] == "intermediary"), None)
    deliverer = next((p for p in persons_summary if p["role"] == "deliverer"), None)

    def pname(p):
        if not p:
            return None
        n = p.get("normalized") or p["name"]
        t = p.get("title")
        o = p.get("office")
        parts = [n]
        if t:
            parts.append(f"({t})")
        if o:
            parts.append(f"[{o}]")
        return " ".join(parts)

    # Build sentences
    en_parts = [f"This tablet {tx_en} {animals_en}."]
    nl_parts = [f"Dit tablet {tx_nl} {animals_nl}."]

    if source:
        en_parts.append(f"From {pname(source)}.")
        nl_parts.append(f"Van {pname(source)}.")
    if deliverer:
        en_parts.append(f"Delivered by {pname(deliverer)}.")
        nl_parts.append(f"Geleverd door {pname(deliverer)}.")
    if receiver:
        en_parts.append(f"Received by {pname(receiver)}.")
        nl_parts.append(f"Ontvangen door {pname(receiver)}.")
    if intermediary:
        en_parts.append(f"Via {pname(intermediary)}.")
        nl_parts.append(f"Via {pname(intermediary)}.")
    if divine_recips:
        gods = ", ".join(d.get("en", d["name"]) for d in divine_recips)
        en_parts.append(f"For {gods}.")
        nl_parts.append(f"Voor {gods}.")
    if result.destination:
        dest_en = result.destination
        en_parts.append(f"Destination: {dest_en}.")
        nl_parts.append(f"Bestemming: {dest_en}.")
    if result.month:
        en_parts.append(f"Dated month {result.month}" +
                        (f", day {result.day}" if result.day else "") + ".")
        nl_parts.append(f"Gedateerd maand {result.month}" +
                        (f", dag {result.day}" if result.day else "") + ".")

    return {"en": " ".join(en_parts), "nl": " ".join(nl_parts)}


def load_database():
    tablets = {}
    with open(DATABASE, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("transliteration", "").strip():
                tablets[row["id_text"]] = row
    return tablets


def build_tablet_index():
    index = []
    with open(EXTRACTED, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("has_transliteration") == "1":
                index.append({
                    "id": row["tablet_id"],
                    "designation": row.get("designation", ""),
                    "date": row.get("date_of_origin", ""),
                    "tx": row.get("transaction_type", ""),
                    "animals": int(row.get("total_animals", 0)),
                    "confidence": row.get("extraction_score", ""),
                })
    return index


class VisualizerHandler(SimpleHTTPRequestHandler):
    tablets = None
    tablet_index = None
    _corpus_stats = None
    _timeline = None
    _animals_timeline = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(BASE_DIR), **kwargs)

    def do_GET(self):
        if self.path == "/api/index":
            self.send_json(self.tablet_index)
        elif self.path.startswith("/api/tablet/"):
            tid = self.path.split("/")[-1]
            if tid in self.tablets:
                row = self.tablets[tid]
                data = annotate_tablet(
                    tid, row["transliteration"], row.get("dates_referenced", "")
                )
                data["designation"] = row.get("designation", "")
                data["date_of_origin"] = row.get("dates_referenced", "")
                self.send_json(data)
            else:
                self.send_error(404, f"Tablet {tid} not found")
        elif self.path.startswith("/api/random"):
            import random
            tid = random.choice(list(self.tablets.keys()))
            row = self.tablets[tid]
            data = annotate_tablet(
                tid, row["transliteration"], row.get("dates_referenced", "")
            )
            data["designation"] = row.get("designation", "")
            data["date_of_origin"] = row.get("dates_referenced", "")
            self.send_json(data)
        elif self.path == "/api/stats":
            self.send_json(self._corpus_stats)
        elif self.path == "/api/timeline":
            self.send_json(self._timeline)
        elif self.path == "/api/animals_timeline":
            self.send_json(self._animals_timeline)
        elif self.path.startswith("/api/animals_cell"):
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            animal = (qs.get("animal") or [""])[0]
            period = (qs.get("period") or [""])[0]
            gran   = (qs.get("granularity") or ["year"])[0]
            self.send_json(get_animals_cell(animal, period, gran))
        else:
            super().do_GET()

    def send_json(self, data):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


def compute_corpus_stats(index):
    from collections import Counter
    tx_counts = Counter()
    conf_counts = Counter()
    total_animals = 0
    for t in index:
        tx_counts[t["tx"] or "unknown"] += 1
        conf_counts[t["confidence"] or "none"] += 1
        total_animals += t["animals"]
    return {
        "total_tablets": len(index),
        "total_animals": total_animals,
        "by_transaction": dict(tx_counts.most_common()),
        "by_confidence": dict(conf_counts),
    }


# ---------------------------------------------------------------------------
# Timeline / officials activity
# ---------------------------------------------------------------------------

# Reign sequence for the corpus span. The y-axis "absolute year" is computed
# as ruler_offset[ruler] + regnal_year. Šulgi data here only covers his last
# years (42–48) since Puzriš-Dagan was founded in Š.39.
RULER_ORDER = [
    ("Šulgi",      "Š",  48),    # 48-year reign
    ("Amar-Suen",  "AS",  9),    # 9 years
    ("Šū-Suen",    "ŠS",  9),    # 9 years
    ("Ibbi-Suen",  "IS", 24),    # 24 years (corpus only reaches IS.02)
]


def _ruler_index(ruler: str) -> int:
    for i, (name, _abbr, _len) in enumerate(RULER_ORDER):
        if name == ruler:
            return i
    return -1


def _period_label(ruler: str, year: int) -> str:
    for name, abbr, _len in RULER_ORDER:
        if name == ruler:
            return f"{abbr}.{year:02d}"
    return f"{ruler}.{year}"


def _split_persons(field: str) -> list[str]:
    if not field:
        return []
    return [n.strip() for n in field.split(";") if n.strip()]


# ---------------------------------------------------------------------------
# Tenure overrides for officials whose role changed mid-corpus.
#
# The OFFICIALS_TO_OFFICE map in drehem_extract.py is one office per name.
# A few officials had distinct, well-attested careers in two bureaus and
# really should be split. The override key is the ATF name; the value is
# an ordered list of phases. Each phase declares the inclusive
# (ruler, regnal_year, month) cut-off and the role the official held
# during that span. Phases are evaluated in order; the first phase whose
# end-date is on or after the tablet's date wins.
#
# A None component matches any value ("up to AS.09 month 7" inclusive →
# any earlier ruler / earlier year, plus AS.09 months 1–7).
#
# Reference (Tsouparopoulou 2013, Sigrist 1992):
#   Intaea served the Shepherds–LuNingirsu family AS.03–AS.09.07 before
#   being promoted to Chief Official from AS.09.08 through IS.02.
# ---------------------------------------------------------------------------

# (ruler_abbr or None, year or None, month or None)
TENURE_PHASES = {
    "in-ta-e3-a": [
        # Up to and including AS.09 month 7 → Shepherds-LuNingirsu
        {"end": ("AS", 9, 7),  "office": "S", "sub_office": "Shepherds-LuNingirsu"},
        # Anything later → Chief Official
        {"end": (None, None, None), "office": "C", "sub_office": "Chief Official"},
    ],
}

_RULER_ABBR = {"Šulgi": "Š", "Amar-Suen": "AS", "Šū-Suen": "ŠS", "Ibbi-Suen": "IS"}
_RULER_RANK = {abbr: i for i, abbr in enumerate(["Š", "AS", "ŠS", "IS"])}


def _date_le(a, b):
    """Compare two (ruler_abbr, year, month) tuples; None on b means infinity."""
    if b == (None, None, None):
        return True
    if b[0] is None:
        return True
    ar = _RULER_RANK.get(a[0], -1)
    br = _RULER_RANK.get(b[0], -1)
    if ar != br:
        return ar < br
    ay = a[1] or 0
    by = b[1] or 0
    if ay != by:
        return ay <= by
    am = a[2] or 0
    bm = b[2] or 99
    return am <= bm


def _resolved_role(name, ruler_abbr, year, month, default_office, default_sub):
    """Apply TENURE_PHASES override and return (office, sub_office)."""
    phases = TENURE_PHASES.get(name)
    if not phases:
        return default_office, default_sub
    here = (ruler_abbr, year or 0, month or 0)
    for ph in phases:
        if _date_le(here, ph["end"]):
            return ph["office"], ph["sub_office"]
    return default_office, default_sub


def compute_timeline_data():
    """Build per-official activity grid + transaction edges.

    For each known official (member of OFFICIALS_TO_OFFICE):
        – tablets per regnal-year period
        – total tablets
        – outgoing edges (this official → receiver) and incoming
          (source → this official) when both ends are recognised officials.

    Officials listed in TENURE_PHASES are split into one synthetic entry
    per phase (e.g. Intaea S → C), each tracked independently with its
    own date range and edges.
    """
    from collections import defaultdict, Counter

    OFFICE_LABELS = {
        "C": "Chief Official",
        "D": "Disbursal Office",
        "S": "Shepherds Office",
        "X": "Dead Animals Office",
    }

    # ORACC name normalization — used to provide a "standardized name" view
    # (e.g. "ab-ba-sa6-ga" → "Abbasaga"). Falls back to the raw ATF token
    # when the official is not in the dictionary.
    try:
        from build_name_dictionary import normalize_name as _norm
    except Exception:
        _norm = None

    def _canon(name: str) -> str:
        if _norm is None:
            return name
        try:
            r = _norm(name)
        except Exception:
            return name
        if r and r.get("canonical"):
            return r["canonical"]
        return name

    def _new_record(synthetic_key, atf_name, office, sub_office, phase_label=None):
        return {
            # `name` is the unique key that edges and click-handlers reference.
            # When an official has split tenure it becomes synthetic
            # (e.g. "in-ta-e3-a#S"); otherwise it is identical to atf_name.
            "name": synthetic_key,
            "atf_name": atf_name,
            "normalized_name": _canon(atf_name),
            "office": office,
            "office_label": OFFICE_LABELS.get(office, office),
            "sub_office": sub_office,
            "phase": phase_label,
            "by_period": Counter(),
            "first_period": None,
            "last_period": None,
            "first_year_idx": None,
            "last_year_idx": None,
            "total_tablets": 0,
            "as_source": 0,
            "as_receiver": 0,
            "as_intermediary": 0,
        }

    # `officials` maps a synthetic key (the ATF name plus an optional phase
    # suffix when the career spans multiple offices) to one record. This is
    # the dictionary the timeline renders. We also keep a reverse index
    # `phase_keys[name]` so the per-tablet date resolver can pick the right
    # bucket without re-scanning.
    officials = {}
    phase_keys = {}   # ATF name -> list of synthetic keys, in phase order

    for name, (office, sub) in OFFICIALS_TO_OFFICE.items():
        if name in TENURE_PHASES:
            keys = []
            for ph in TENURE_PHASES[name]:
                key = f"{name}#{ph['office']}"  # e.g. in-ta-e3-a#S
                officials[key] = _new_record(
                    synthetic_key=key,
                    atf_name=name,
                    office=ph["office"],
                    sub_office=ph["sub_office"],
                    phase_label=ph["office"],
                )
                keys.append(key)
            phase_keys[name] = keys
        else:
            officials[name] = _new_record(name, name, office, sub)
            phase_keys[name] = [name]

    edges = Counter()   # (source, receiver) -> tablet count
    periods_seen = set()

    def _resolve_key(name, ruler_abbr, year, month):
        """Pick the right phase key for a given ATF name and tablet date."""
        keys = phase_keys.get(name)
        if not keys:
            return None
        if len(keys) == 1:
            return keys[0]
        # Multi-phase: walk the override list, find the first phase that contains the date.
        here = (ruler_abbr, year or 0, month or 0)
        for ph, key in zip(TENURE_PHASES[name], keys):
            if _date_le(here, ph["end"]):
                return key
        return keys[-1]

    with open(EXTRACTED, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ruler = row.get("ruler", "")
            r_idx = _ruler_index(ruler)
            if r_idx < 0:
                continue
            try:
                year = int(row.get("regnal_year") or 0)
            except ValueError:
                year = 0
            if year <= 0:
                continue
            try:
                month = int(row.get("month_number") or 0)
            except ValueError:
                month = 0
            ruler_abbr = _RULER_ABBR.get(ruler, ruler)

            label = _period_label(ruler, year)
            # year_idx must order tablets across reigns. A flat sum collides
            # when a tablet is dated past its reign's nominal length
            # (e.g. AS.10 vs ŠS.01 — Amar-Suen ruled 9 years, but a few
            # tablets carry an AS.10 date). A (ruler_idx, year) tuple keeps
            # AS.10 between AS.09 and ŠS.01 where it belongs.
            year_idx = (r_idx, year)
            periods_seen.add((year_idx, label))

            sources = _split_persons(row.get("source", ""))
            receivers = _split_persons(row.get("receiver", ""))
            intermediaries = _split_persons(row.get("intermediary", ""))

            # Resolve each name to a *phase key* — for officials with split
            # tenures (e.g. Intaea), this routes the tablet to whichever
            # bureau they served in at the time of the tablet.
            src_keys = [k for s in sources if (k := _resolve_key(s, ruler_abbr, year, month))]
            rcv_keys = [k for r in receivers if (k := _resolve_key(r, ruler_abbr, year, month))]
            int_keys = [k for m in intermediaries if (k := _resolve_key(m, ruler_abbr, year, month))]

            present = set()
            for k in src_keys:
                present.add(k); officials[k]["as_source"] += 1
            for k in rcv_keys:
                present.add(k); officials[k]["as_receiver"] += 1
            for k in int_keys:
                present.add(k); officials[k]["as_intermediary"] += 1

            for k in present:
                o = officials[k]
                o["by_period"][label] += 1
                o["total_tablets"] += 1
                if o["first_year_idx"] is None or year_idx < o["first_year_idx"]:
                    o["first_year_idx"] = year_idx
                    o["first_period"] = label
                if o["last_year_idx"] is None or year_idx > o["last_year_idx"]:
                    o["last_year_idx"] = year_idx
                    o["last_period"] = label

            # Network edges: source → receiver pairs of known officials,
            # using the resolved phase keys so a Shepherd-era Intaea is
            # distinct from a Chief-era Intaea in the network.
            for s in src_keys:
                for r in rcv_keys:
                    if r != s:
                        edges[(s, r)] += 1

    periods = [label for _idx, label in sorted(periods_seen)]

    # Convert Counters to dicts and drop officials with zero activity
    out_officials = []
    for o in officials.values():
        if o["total_tablets"] == 0:
            continue
        o["by_period"] = dict(o["by_period"])
        # Year-idx tuples (r_idx, year) don't survive JSON round-trips
        # cleanly; the front-end only uses these for span-band sizing,
        # so we collapse them to a single integer for transport.
        if o["first_year_idx"] is not None:
            r_idx0, y0 = o["first_year_idx"]
            o["first_year_idx"] = r_idx0 * 100 + y0
            r_idx1, y1 = o["last_year_idx"]
            o["last_year_idx"] = r_idx1 * 100 + y1
        out_officials.append(o)

    # Sort officials: by office then by first-active period
    office_rank = {"C": 0, "D": 1, "S": 2, "X": 3}
    out_officials.sort(key=lambda o: (
        office_rank.get(o["office"], 99),
        o["sub_office"],
        o["first_year_idx"] if o["first_year_idx"] is not None else 99999,
        o["name"],
    ))

    out_edges = [
        {"source": s, "target": t, "count": c}
        for (s, t), c in edges.most_common()
    ]

    return {
        "periods": periods,
        "officials": out_officials,
        "edges": out_edges,
        "office_labels": OFFICE_LABELS,
    }


# ---------------------------------------------------------------------------
# Animals timeline / livestock breakdown
# ---------------------------------------------------------------------------

# Coarse category grouping for the stacked-area view. Each animal term in
# animals_detail is matched against its prefix (before the first hyphen
# or space) and routed to one of these buckets. Anything not listed falls
# under "other". Categories are intentionally coarse — fine-grained
# species/qualifier distinctions surface in the drill-down.
ANIMAL_CATEGORIES = {
    "sheep_adult":  {"label": "Sheep (adult)",  "color": "#22c55e",
                     "members": {"udu", "u8"}},
    "sheep_young":  {"label": "Sheep (young)",  "color": "#86efac",
                     "members": {"sila4", "kir11"}},
    "goat_adult":   {"label": "Goats (adult)",  "color": "#0ea5e9",
                     "members": {"masz2", "ud5"}},
    "goat_young":   {"label": "Goats (young)",  "color": "#7dd3fc",
                     "members": {"masz", "masz-da3"}},
    "cattle_adult": {"label": "Cattle (adult)", "color": "#a855f7",
                     "members": {"gu4", "ab2"}},
    "cattle_young": {"label": "Cattle (young)", "color": "#d8b4fe",
                     "members": {"amar"}},
    "equids":       {"label": "Equids",         "color": "#f59e0b",
                     "members": {"ansze", "anše", "{ansze}kunga2"}},
    "other":        {"label": "Other",          "color": "#94a3b8",
                     "members": set()},
}


def _animal_base(animal_term: str) -> str:
    """Strip qualifiers and curly-brace determinatives to a base species token.

    'udu-niga'        → 'udu'
    'udu'             → 'udu'
    'masz2-gal-niga'  → 'masz2'
    'amar masz-da3'   → 'amar'
    '{ansze}kunga2'   → '{ansze}kunga2'  (preserve composite)
    """
    if not animal_term:
        return ""
    # Whole-token composite? keep verbatim for equids etc.
    if animal_term.startswith("{") and "}" in animal_term:
        return animal_term
    # Otherwise take the part before the first hyphen or space.
    head = re.split(r"[-\s]", animal_term, 1)[0]
    return head


def _animal_category(base: str) -> str:
    for key, meta in ANIMAL_CATEGORIES.items():
        if base in meta["members"]:
            return key
    return "other"


# Pattern matches `count×animal-qualifier` entries.
# count is either an integer or `[imp:N]` (deterministic imputation).
_ANIMAL_ENTRY_RE = re.compile(r"(\[imp:(\d+)\]|(\d+))\s*[×x]\s*([^,]+)")

# Cached cell-level contributions populated by compute_animals_timeline().
# Keyed by animal_base -> period_label -> [{tablet, count, animal}].
# Looked up by /api/animals_cell to lazy-load drill-down data.
ANIMAL_CONTRIB_YEAR = {}
ANIMAL_CONTRIB_MONTH = {}


def _parse_animals_detail(detail: str):
    """Yield (count, animal_term) tuples from an animals_detail cell."""
    for m in _ANIMAL_ENTRY_RE.finditer(detail or ""):
        if m.group(2):
            count = int(m.group(2))
        else:
            count = int(m.group(3))
        animal = m.group(4).strip()
        if animal:
            yield count, animal


def compute_animals_timeline():
    """Build per-period × animal activity grid for the livestock view.

    Two granularities are produced:
        – year-level   (e.g. "AS.05")  — always populated
        – month-level  (e.g. "AS.05.07") — populated only when the tablet's
          month_number is set

    Per (animal × period) cells include the top-50 contributing tablets so
    the front-end can show a direct drill-down without needing a second
    round-trip.
    """
    from collections import Counter, defaultdict

    # animal_base -> {period_label: [{tablet, count, animal}]}
    # These contributions are kept module-level so the /api/animals_cell
    # endpoint can serve drill-downs lazily without recomputing.
    contrib_year = defaultdict(lambda: defaultdict(list))
    contrib_month = defaultdict(lambda: defaultdict(list))
    by_period_total_y = Counter()
    by_period_total_m = Counter()
    tx_by_period_y = defaultdict(Counter)
    tablets_by_period_y = Counter()
    tablets_by_period_m = Counter()
    period_idx_year = {}
    period_idx_month = {}

    with open(EXTRACTED, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ruler = row.get("ruler", "")
            r_idx = _ruler_index(ruler)
            if r_idx < 0:
                continue
            try:
                year = int(row.get("regnal_year") or 0)
            except ValueError:
                year = 0
            if year <= 0:
                continue
            try:
                month = int(row.get("month_number") or 0)
            except ValueError:
                month = 0

            label_y = _period_label(ruler, year)
            label_m = f"{label_y}.{month:02d}" if 1 <= month <= 13 else None
            period_idx_year[label_y] = (r_idx, year)
            if label_m is not None:
                period_idx_month[label_m] = (r_idx, year, month)

            entries = list(_parse_animals_detail(row.get("animals_detail", "")))
            if not entries:
                continue

            tablets_by_period_y[label_y] += 1
            if label_m: tablets_by_period_m[label_m] += 1
            tx = row.get("transaction_type") or "unknown"
            tx_by_period_y[label_y][tx] += 1

            for count, animal in entries:
                base = _animal_base(animal)
                rec = {"tablet": row["tablet_id"], "count": count, "animal": animal}
                contrib_year[base][label_y].append(rec)
                if label_m: contrib_month[base][label_m].append(rec)
                by_period_total_y[label_y] += count
                if label_m: by_period_total_m[label_m] += count

    periods_year  = sorted(period_idx_year.keys(),  key=lambda p: period_idx_year[p])
    periods_month = sorted(period_idx_month.keys(), key=lambda p: period_idx_month[p])

    # Per-animal aggregates. Per-cell tablet lists are NOT included in the
    # main payload (would be ~3 MB JSON); callers fetch them on-demand via
    # /api/animals_cell which reads from the cached contrib_* dicts.
    def _summarize(contribs_by_period):
        out_count, out_tabs, out_max, total_c, total_t = {}, {}, {}, 0, 0
        for period, contribs in contribs_by_period.items():
            psum = sum(c["count"] for c in contribs)
            total_c += psum
            total_t += len(contribs)
            out_count[period] = psum
            out_tabs[period] = len(contribs)
            out_max[period] = max(c["count"] for c in contribs)
        return out_count, out_tabs, out_max, total_c, total_t

    animals_out = []
    overall_top = {}  # animal -> top 8 contributors across the corpus
    for base, year_buckets in contrib_year.items():
        cy, ty, mxy, tc, tt = _summarize(year_buckets)
        cm, tm, mxm, _, _ = _summarize(contrib_month.get(base, {}))
        # Overall top contributors (across the whole corpus, all periods)
        all_contribs = [c for contribs in year_buckets.values() for c in contribs]
        all_contribs.sort(key=lambda c: -c["count"])
        overall_top[base] = all_contribs[:8]
        animals_out.append({
            "name": base,
            "category": _animal_category(base),
            "label": base,
            "by_period":          cy,
            "by_period_tablets":  ty,
            "by_period_max":      mxy,
            "by_month":           cm,
            "by_month_tablets":   tm,
            "by_month_max":       mxm,
            "top_contributors":   overall_top[base],  # corpus-wide top 8
            "total_count":        tc,
            "total_tablets":      tt,
        })

    # Drop singleton animals
    animals_out = [a for a in animals_out if a["total_tablets"] >= 2]
    animals_out.sort(key=lambda a: (-a["total_tablets"], -a["total_count"]))

    # Sheep-to-tablet ratio per period — outlier spotter
    sheep_to_tablet_y = {}
    for p, n_tab in tablets_by_period_y.items():
        if n_tab > 0:
            sheep_to_tablet_y[p] = round(by_period_total_y[p] / n_tab, 1)

    categories_out = {
        k: {"label": v["label"], "color": v["color"]}
        for k, v in ANIMAL_CATEGORIES.items()
    }

    # Publish contributions for the lazy /api/animals_cell endpoint.
    global ANIMAL_CONTRIB_YEAR, ANIMAL_CONTRIB_MONTH
    ANIMAL_CONTRIB_YEAR = {a: dict(d) for a, d in contrib_year.items()}
    ANIMAL_CONTRIB_MONTH = {a: dict(d) for a, d in contrib_month.items()}

    return {
        "periods":          periods_year,
        "periods_month":    periods_month,
        "animals":          animals_out,
        "categories":       categories_out,
        "by_period_total":  dict(by_period_total_y),
        "by_month_total":   dict(by_period_total_m),
        "by_period_tx":     {k: dict(v) for k, v in tx_by_period_y.items()},
        "tablets_by_period": dict(tablets_by_period_y),
        "tablets_by_month":  dict(tablets_by_period_m),
        "sheep_to_tablet":   sheep_to_tablet_y,
    }


def get_animals_cell(animal: str, period: str, granularity: str = "year", limit: int = 100):
    """Drill-down: return tablets that contributed to one (animal, period) cell.

    `granularity` is "year" or "month". `period` is the period label
    (e.g. "AS.05" for year, "AS.05.07" for month).
    """
    src = ANIMAL_CONTRIB_MONTH if granularity == "month" else ANIMAL_CONTRIB_YEAR
    cell = src.get(animal, {}).get(period, [])
    if not cell:
        return {"animal": animal, "period": period, "granularity": granularity,
                "total_count": 0, "total_tablets": 0, "tablets": []}
    sorted_cell = sorted(cell, key=lambda c: -c["count"])[:limit]
    total_count = sum(c["count"] for c in cell)
    return {
        "animal": animal,
        "period": period,
        "granularity": granularity,
        "total_count": total_count,
        "total_tablets": len(cell),
        "tablets": sorted_cell,
    }


def serve(port=8585):
    print("Loading tablets...")
    VisualizerHandler.tablets = load_database()
    VisualizerHandler.tablet_index = build_tablet_index()
    VisualizerHandler._corpus_stats = compute_corpus_stats(VisualizerHandler.tablet_index)
    VisualizerHandler._timeline = compute_timeline_data()
    VisualizerHandler._animals_timeline = compute_animals_timeline()
    print(f"Loaded {len(VisualizerHandler.tablets)} tablets, "
          f"{len(VisualizerHandler._timeline['officials'])} active officials, "
          f"{len(VisualizerHandler._animals_timeline['animals'])} animal terms")

    server = HTTPServer(("", port), VisualizerHandler)
    print(f"\nTablet Visualizer running on http://localhost:{port}/tablet_vis.html")
    print("Press Ctrl+C to stop.\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.server_close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tablet extraction visualizer")
    parser.add_argument("--tablet", help="Export single tablet annotation as JSON")
    parser.add_argument("--port", type=int, default=8585)
    args = parser.parse_args()

    if args.tablet:
        tablets = load_database()
        if args.tablet in tablets:
            row = tablets[args.tablet]
            data = annotate_tablet(
                args.tablet, row["transliteration"], row.get("dates_referenced", "")
            )
            print(json.dumps(data, indent=2, ensure_ascii=False))
        else:
            print(f"Tablet {args.tablet} not found")
    else:
        serve(args.port)
