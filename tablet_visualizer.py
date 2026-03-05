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
    strip_atf_damage, parse_numeral, compute_confidence,
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
    "masz2": ("billy goat", "bok"),
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
    # Transaction types
    "mu-kux(DU)": ("delivery (incoming)", "levering (inkomend)"),
    "ba-zi": ("expenditure (outgoing)", "uitgave (uitgaand)"),
    "i3-dab5": ("transfer (took over)", "overdracht (overgenomen)"),
    "szu": ("hand", "hand"),
    "ba-ti": ("received", "ontvangen"),
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
    # Common non-animal terms
    "ensi2": ("governor", "gouverneur"),
    "sukkal": ("minister", "minister"),
    "szagina": ("general", "generaal"),
    "lugal": ("king", "koning"),
    "e2": ("house/temple", "huis/tempel"),
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
    "sza3": ("heart/inside", "hart/binnenin"),
    "bala": ("rotating office", "roterend kantoor"),
}

# Dutch month translations
MONTH_TRANSLATIONS_NL = {
    1: "maand 1", 2: "maand 2", 3: "maand 3", 4: "maand 4",
    5: "maand 5", 6: "maand 6", 7: "maand 7", 8: "maand 8",
    9: "maand 9", 10: "maand 10", 11: "maand 11", 12: "maand 12",
}

TRANSACTION_KEYWORDS = {
    "mu-kux(DU)": "delivery",
    "ba-zi": "expenditure",
    "i3-dab5": "transfer",
    "szu ba-ti": "receipt",
}

ROLE_KEYWORDS = {
    "ki": "source_marker",
    "-ta": "source_suffix",
    "giri3": "intermediary_marker",
    "maszkim": "commissioner_marker",
    "kiszib3": "sealer_marker",
}

STRUCTURAL_KEYWORDS = {
    "szu-nigin": "summary",
    "szunigin": "summary",
    "sza3-bi-ta": "subsection_marker",
    "sa2-du11": "regular_offering",
    "nig2-dab5": "provisions",
    "iti": "month_marker",
    "u4": "day_marker",
}
# Note: "mu" removed — it's only structural at the START of year-name lines.
# In other positions it means "for" (dative) or is part of person names.

DEITY_PREFIX = "{d}"
ALL_OFFICIALS = set(k.lower() for k in OFFICIALS_TO_OFFICE)

QUALIFIERS = {
    "niga": "fattened", "u2": "grass-fed", "a-lum": "breeding male",
    "bar-gal": "large breed", "bar-su-ga": "fleece restored",
    "nita2": "male", "munus": "female", "nita": "male",
    "masz2": "billy goat", "sila4": "lamb", "gal": "large",
    "tur": "small/young", "amar": "calf/young",
    "ba-usz2": "dead", "ba-ug7": "dead", "usz2": "dead",
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


def _matches_transaction_keyword(token_clean: str, tokens: list[str], idx: int) -> bool:
    """Return True if token at idx starts a transaction keyword match.

    Uses exact token equality (not substring), which prevents false positives
    such as 'ba-ti' inside 'ba-ba-ti' matching the second word of 'szu ba-ti'.
    """
    from drehem_extract import strip_atf_damage as _sad
    for kw in TRANSACTION_KEYWORDS:
        parts = kw.split()
        if token_clean == parts[0]:
            # single-token keyword: always a match
            if len(parts) == 1:
                return True
            # multi-token keyword: check subsequent tokens
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
    for kw in TRANSACTION_KEYWORDS:
        parts = kw.split()
        if token_clean != parts[0]:
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


def annotate_tablet(tablet_id, transliteration, date_of_origin=""):
    """Produce token-level annotations for visualization."""
    result = extract_tablet(tablet_id, transliteration, date_of_origin)
    issues = detect_issues(tablet_id, transliteration, result)
    confidence = compute_confidence(result, True)
    edge_total = _extract_edge_total(transliteration)

    # Build person name lookup
    person_names = set()
    person_roles = {}
    for p in result.persons:
        n = p.name.lower().strip()
        person_names.add(n)
        person_roles[n] = p.role

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
        while i < len(tokens_raw):
            token = tokens_raw[i]
            token_clean = strip_atf_damage(token).lower()
            role = "unknown"
            detail = ""

            if NUMERAL_PATTERN.match(token_clean):
                val = parse_numeral(token_clean)
                role = "numeral"
                detail = str(val)

            elif token_clean in ANIMAL_TERMS:
                role = "animal"
                detail = ANIMAL_TERMS[token_clean]

            elif token_clean in QUALIFIERS:
                role = "qualifier"
                detail = QUALIFIERS[token_clean]

            elif any(token_clean == k or content[content.find(token):].startswith(k) for k in TRANSACTION_KEYWORDS):
                matched_tx = None
                for k in TRANSACTION_KEYWORDS:
                    if token_clean == k.split()[0] if " " in k else token_clean == k:
                        matched_tx = k
                        break
                if matched_tx:
                    role = "transaction"
                    detail = TRANSACTION_KEYWORDS[matched_tx]
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

            elif token_clean in person_names:
                role = "person"
                detail = person_roles.get(token_clean, "")
            elif any(token_clean.startswith(pn + "-") or token_clean.startswith(pn) for pn in person_names if len(pn) > 3 and token_clean.startswith(pn)):
                matched_pn = next(pn for pn in person_names if token_clean.startswith(pn) and len(pn) > 3)
                role = "person"
                detail = person_roles.get(matched_pn, "") + f" ({matched_pn})"
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

            elif _matches_transaction_keyword(token_clean, tokens_raw, i):
                matched_tx = _get_transaction_keyword(token_clean, tokens_raw, i)
                if matched_tx:
                    role = "transaction"
                    detail = TRANSACTION_KEYWORDS[matched_tx]
                    # Skip extra tokens consumed by multi-token keywords
                    i += len(matched_tx.split()) - 1
                else:
                    role = "text"

            elif token_clean in STRUCTURAL_KEYWORDS:
                role = "structural_keyword"
                detail = STRUCTURAL_KEYWORDS[token_clean]

            elif DEITY_PREFIX in token:
                # Deities inside year-name lines are NOT divine recipients
                # — they are part of royal/year formulae (e.g. mu {d}szu-{d}suen ...)
                if cleaned_lower.startswith("mu ") and any(
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

            elif token_clean in ("ki", "giri3", "maszkim", "kiszib3"):
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

            elif token in ("x", "...") or token.startswith("[") or token.endswith("]") or token == "[...]":
                role = "damage"
                detail = "broken/illegible"

            if role == "unknown":
                role = "text"

            en, nl = get_translation(token_clean, role)
            # For roles with detail, use detail as English if no dict entry
            if not en and detail:
                en = detail

            annotated_tokens.append({
                "text": token,
                "role": role,
                "detail": detail,
                "en": en,
                "nl": nl,
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
            "day": result.day,
            "year": result.year,
            "has_summary_line": result.has_summary_line,
            "confidence": confidence,
            "preservation": result.damage.preservation if result.damage else None,
            "divine_recipients": result.divine_recipients,
            "divine_recipients_translated": divine_recips,
            "destination": result.destination,
        },
        "issues": issues,
    }


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
                    "confidence": row.get("extraction_confidence", ""),
                })
    return index


class VisualizerHandler(SimpleHTTPRequestHandler):
    tablets = None
    tablet_index = None
    _corpus_stats = None

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


def serve(port=8585):
    print("Loading tablets...")
    VisualizerHandler.tablets = load_database()
    VisualizerHandler.tablet_index = build_tablet_index()
    VisualizerHandler._corpus_stats = compute_corpus_stats(VisualizerHandler.tablet_index)
    print(f"Loaded {len(VisualizerHandler.tablets)} tablets")

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
