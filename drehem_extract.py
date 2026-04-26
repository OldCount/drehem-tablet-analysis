#!/usr/bin/env python3
"""
Extract structured data from Drehem tablet transliterations.

Parses ATF-format transliterations to identify:
- Transaction type (mu-kux, ba-zi, i3-dab5, szu ba-ti)
- Persons and their roles (source, receiver, intermediary, commissioner)
- Animals with counts and qualifiers
- Date components (month, day, year name)
- Normalized (canonical) names via ORACC lemmatization data
"""

import re
import csv
from pathlib import Path
from dataclasses import dataclass, field

# ORACC name normalization — auto-loads the dictionary on first use.
# Run `python3 build_name_dictionary.py` once to build the dictionary.
try:
    from build_name_dictionary import normalize_name as _oracc_normalize
    _HAS_NAME_DICT = True
except ImportError:
    _HAS_NAME_DICT = False


# ---------------------------------------------------------------------------
# Constants: known terms from Puzrish-Dagan administrative corpus
# ---------------------------------------------------------------------------

TRANSACTION_PATTERNS = {
    "szu ba-ti":   "to accept",
    "mu-kux(DU)":  "delivery",
    "i3-dab5":     "transfer",
    "ba-zi":       "expenditure",
}

OFFICIAL_TITLES = [
    "dub-sar", "sukkal", "sanga", "muhaldim", "ra2-gaba",
    "aga3-us2", "kuruszda", "ensi2", "lu2-kin-gi4-a",
    "szagina", "nu-banda3", "ugula", "szar2-ra-ab-du",
    "sza3-tam", "sipa", "unu3", "nagar",
]

# Officials → office mapping (based on Tsouparopoulou 2013 and Liu)
# C = Chief Official, D = Disbursal Office, S = Shepherds Office, X = Dead Animals
#
# Sub-bureau labels follow Tsouparopoulou's branch reconstruction:
#   Disbursal-NakabtumA/B, Disbursal-Tummal, Disbursal-NippurUr,
#   Disbursal-RoyalCourt, Disbursal-ExoticAnimals
#   Shepherds-LuNingirsu, Shepherds-Enlila, Shepherds (shepherd specialists)
#   DeadAnimals-Kitchen, DeadAnimals-Hides
#
# KNOWN CONFLICTS — cannot be resolved by name alone:
#   in-ta-e3-a: served S (Shepherds-LuNingirsu, AS.03–09) before becoming
#     C (Chief Official, AS.09–IS.02). Mapped to C (later, higher role).
#     Tablets from AS.03–09 will be mis-assigned to C.
#   lu2-dingir-ra: two distinct officials share this ATF name:
#     (1) Lu-dingira son of Inim-Šara → Disbursal-NakabtumB (Š.45–AS.03)
#     (2) Lu-dingira son of Urdu-Hula → Disbursal-ExoticAnimals (Š.43–AS.09)
#     Mapped to NakabtumB (primary usage). Exotic-animals tablets will be
#     mis-assigned. Prosopographic disambiguation requires filiation data.
#   na-lu: active in both Tummal (Š.44) and Nippur/Ur branches (Š.28–ŠS.05).
#     Mapped to Tummal; Nippur/Ur tablets by Nalu will be mis-assigned.
OFFICIALS_TO_OFFICE = {
    # ── Chief Officials (C) ───────────────────────────────────────────────────
    "na-sa6":                  ("C", "Chief Official"),   # Nasa,         Š.42–AS.02
    "ab-ba-sa6-ga":            ("C", "Chief Official"),   # Abba-saga,    AS.02–AS.09
    "lugal-amar-ku":           ("C", "Chief Official"),   # Lugal-amar-ku, AS.08 temp
    "in-ta-e3-a":              ("C", "Chief Official"),   # Intaea,       AS.09–IS.02 (also S AS.03–09; see conflict note)

    # ── Disbursal Office – Nakabtum A (D) ────────────────────────────────────
    "a-hu-ni":                 ("D", "Disbursal-NakabtumA"),  # AHuni,       Š.42–AS.02
    "{d}szul-gi-a-a-gu":       ("D", "Disbursal-NakabtumA"),  # Šulgi-ayaĝu, Š.44–AS.06
    "szu-ma-ma":               ("D", "Disbursal-NakabtumA"),  # Šu-Mama,     AS.06–AS.08
    "zu-ba-ga":                ("D", "Disbursal-NakabtumA"),  # Zubaga,      AS.08–ŠS.01
    "ur-{d}nanna":             ("D", "Disbursal-NakabtumA"),  # Ur-Nanna,    successor
    "du-du":                   ("D", "Disbursal-NakabtumA"),  # Du'udu,      successor
    "puzur4-{d}en-lil2":       ("D", "Disbursal-NakabtumA"),  # Puzur-Enlil, successor

    # ── Disbursal Office – Nakabtum B (D) ────────────────────────────────────
    "lu2-dingir-ra":           ("D", "Disbursal-NakabtumB"),  # Lu-dingira (son of Inim-Šara), Š.45–AS.03 (see conflict note)
    "a-hu-we-er":              ("D", "Disbursal-NakabtumB"),  # AHu-Wer,     AS.03–AS.08; ŠS.02–07
    "igi-{d}en-lil2-sze3":     ("D", "Disbursal-NakabtumB"),  # Igi-Enlilše, AS.08–ŠS.02
    "ib-ni-{d}suen":           ("D", "Disbursal-NakabtumB"),  # Ibni-Suen,   successor
    "{d}szul-gi-i-li2":        ("D", "Disbursal-NakabtumB"),  # Šulgi-ilī,   successor (ATF uncertain)

    # ── Disbursal Office – Tummal Branch (D) ─────────────────────────────────
    "asz-ni-u3":               ("D", "Disbursal-Tummal"),     # Ašniu,       Š.39–Š.44
    "na-lu":                   ("D", "Disbursal-Tummal"),     # Nalu,        Š.44; also Nippur/Ur (see conflict note)
    "en-dingir-gu":            ("D", "Disbursal-Tummal"),     # En-dingirgu, Š.45–AS.09
    "en-lil2-zi-sza3-gal":     ("D", "Disbursal-Tummal"),     # Enlil-zi-ša-ĝal (ATF uncertain)
    "{d}szul-gi-si-sa2":       ("D", "Disbursal-Tummal"),     # Šulgi-sisa   (ATF uncertain)
    "lu2-sa3-ga":              ("D", "Disbursal-Tummal"),     # Lu-saga      (ATF uncertain)
    "ku-ru-ub-i-la-ak":        ("D", "Disbursal-Tummal"),     # Kurub-ilak   (Akkadian name; ATF uncertain)

    # ── Disbursal Office – Nippur/Ur Branch (D) ──────────────────────────────
    "a-ba-{d}en-lil2-gin7":    ("D", "Disbursal-NippurUr"),   # Aba-Enlilgin, ŠS.06–IS.02

    # ── Disbursal Office – Royal Court Branch (D) ────────────────────────────
    "tu-ra-am-{d}da-gan":      ("D", "Disbursal-RoyalCourt"), # Turam-Dagan,  AS.04–AS.07
    "uta-mi-szar-am":          ("D", "Disbursal-RoyalCourt"), # Uta-mišaram,  AS.04–AS.08
    "ta-hi-isz-a-tal":         ("D", "Disbursal-RoyalCourt"), # Tahiš-atal,   AS.04–ŠS.06 (Akkadian name; ATF uncertain)

    # ── Disbursal Office – Rare/Exotic Animals Branch (D) ────────────────────
    "su-ka-li":                ("D", "Disbursal-ExoticAnimals"),  # Sukalli, Š.38–AS.02; ŠS.03–IS.02 (ATF uncertain)

    # ── Disbursal – branch uncertain ─────────────────────────────────────────
    "{d}szul-gi-a-a-mu":       ("D", "Disbursal"),            # Šulgi-a'amu (branch not established)

    # ── Shepherds Office – Family of Lu-Ningirsu (S) ─────────────────────────
    "ur-ku3-nun-na":           ("S", "Shepherds-LuNingirsu"), # Ur-ku-nuna, Š.41–AS.08; ŠS.04–IS.02
    "du-ga":                   ("S", "Shepherds-LuNingirsu"), # Duga,       AS.04–IS.02
    "ab-ba-kal-la":            ("S", "Shepherds-LuNingirsu"), # Abba-kala,  ŠS.02–IS.02
    # in-ta-e3-a served this family AS.03–09 before becoming Chief Official (mapped to C above)

    # ── Shepherds Office – Family of Enlila (S) ──────────────────────────────
    "en-lil2-la":              ("S", "Shepherds-Enlila"),     # Enlila (son of Ikalla), Š.41–ŠS.02
    "lugal-me-lam2":           ("S", "Shepherds-Enlila"),     # Lugal-melam,   ŠS.03–IS.01
    "lugal-he2-gal2":          ("S", "Shepherds-Enlila"),     # Lugal-hegal,   Š.47–AS.07
    "ur-sa6-ga":               ("S", "Shepherds-Enlila"),     # Ur-saga,       from Š.42
    "ur-szu-ga-lam-ma":        ("S", "Shepherds-Enlila"),     # Ur-šugalama,   AS.01–ŠS.07
    "lu2-{d}nin-gir2-su":      ("S", "Shepherds-Enlila"),     # Lu-Ningirsu,   ŠS.07–IS.01
    "lu2-{d}suen":             ("S", "Shepherds-Enlila"),     # Lu-Suen,       ŠS.08–IS.02

    # ── Shepherds Office – Shepherd Specialists (S) ───────────────────────────
    "ur-mes":                  ("S", "Shepherds"),            # Ur-mes (family of Lana)
    "u3-de3-nig2-sa2-ga":      ("S", "Shepherds"),            # Ude-nig-saga   (ATF uncertain)
    "a-a-la-gu3":              ("S", "Shepherds"),            # A'alaĝu (son of Lana; ATF uncertain)
    "a-ia3-dingir":            ("S", "Shepherds"),            # Aya-dingir
    "za-zi":                   ("S", "Shepherds"),            # Zazi,          AS.08–IS.02
    "lugal-ezem":              ("S", "Shepherds"),            # Lugal-ezem,    AS.03–IS.01
    "a-ha-ni-szu":             ("S", "Shepherds"),            # Aha-nišu,      AS.06–ŠS.03

    # ── Dead Animals Office – Kitchen Department (X) ─────────────────────────
    "be-li2-a-ri-ik":          ("X", "DeadAnimals-Kitchen"),  # Beli-arik,    Š.42–Š.43
    "ur-nigar{gar}":           ("X", "DeadAnimals-Kitchen"),  # Ur-niĝar,     Š.43–AS.03
    "ur-nigar":                ("X", "DeadAnimals-Kitchen"),  # variant spelling
    "{d}szul-gi-iri-mu":       ("X", "DeadAnimals-Kitchen"),  # Šulgi-iriĝu,  AS.03–IS.02

    # ── Dead Animals Office – Hides & Carcasses Department (X) ───────────────
    "nur-{d}suen":             ("X", "DeadAnimals-Hides"),    # Nur-Suen,     AS.02–ŠS.03
    "lu2-kal-la":              ("X", "DeadAnimals-Hides"),    # Lukalla,      ŠS.04–ŠS.09
}

# Known deities that appear as recipients in offering texts
KNOWN_DEITIES = {
    "{d}en-lil2", "{d}nin-lil2", "{d}nanna", "{d}inanna",
    "{d}utu", "{d}nusku", "{d}nin-urta", "{d}nin-gir2-su",
    "{d}ba-ba6", "{d}szara2", "{d}nin-sun2", "{d}nin-gal",
    "{d}da-gan", "{d}nin-tin-ug5-ga", "{d}nansze",
    "{d}iszkur", "{d}nin-a-zu", "{d}nin-hur-sag",
    "{d}be-la-at-suh-ner", "{d}be-la-at-dar-ra-ba-an",
    "an-nu-ni-tum", "{d}ul-ma-szi-tum", "{d}na-na-a",
    "{d}ha-ia3", "{d}me-ki-gal2",
}

# Destination / purpose categories
DESTINATION_CATEGORIES = {
    "e2-muhaldim":      "kitchen",
    "e2-kiszib3-ba":    "warehouse",
    "e2-gal":           "palace",
    "e2-uz-ga":         "e-uzga",
    "aga3-us2-e-ne":    "soldiers",
    "kas4-ke4-ne":      "runners",
    "kas4-e-ne":        "runners",
    "gar3-du-e-ne":     "guards",
    "ur-gi7-ra":        "dogs",
    "szuku-ra-ke4-ne":  "prebend-holders",
    "bala-a":           "bala-obligation",
    # Sacred sites
    "du6-ku3":          "sacred-site",
    "du6-ku3-ga":       "sacred-site",
    "tum-ma-al":        "sacred-site",    # Tummal
    # Palace variants
    "e2-gal-la":        "palace",
    # Sheephouse
    "e2-udu":           "sheephouse",
    "e2-udu-sag":       "sheephouse",
    "e2-udu-ka":        "sheephouse",
    "e2-udu-niga":      "fattening-house",
    # Cities (for ensi2 CITY{ki} patterns and giri3 lines)
    "nibru{ki}":        "city",      # Nippur
    "nibru":            "city",
    "uri2{ki}":         "city",      # Ur
    "uri2":             "city",
    "lagasz{ki}":       "city",      # Lagash
    "umma{ki}":         "city",      # Umma
    "irisagrig{ki}":    "city",      # Irisagrig
    "hu-ur5-ti{ki}":    "city",      # Hurti
    "tummal{ki}":       "sacred-site",  # Tummal
    "tummal":           "sacred-site",
}

# Bala: rotating administrative office delivering animals to central stockyard.
BALA_PATTERN = re.compile(
    r"bala\s+(?P<name>[\w{}-]+(?:\s+[\w{}-]+)*)\s+ensi2"
)

# Non-animal terms that follow numerals. Suppress false 'unrecognized after numeral'.
MEASURE_TERMS = {
    # Volume / capacity
    "barig", "ban2", "sila3", "gin2", "gur",
    # Grain / flour / foodstuff
    "sze", "zid2", "ninda2", "zi3", "zi3-gu", "dabin", "ziz2",
    # Dairy / fat  (NB: "ga" is also an animal qualifier = "suckling";
    #   context determines meaning.  The visualizer handles disambiguation.)
    "ga", "ga-sze-a", "i3",
    # Beer
    "kasz", "kasz-saga", "kasz-du",
    # Weight
    "ma-na", "gu2",
    # Metal / precious material
    "ku3-babbar", "ku3-sig17", "{uruda}ha-bu3-da",
    # Wood / reed / bundles
    "gesz-da", "sa", "gi",
    # Textiles
    "tug2",
    # Containers / objects / vessels
    "har", "dug", "bur-zi",
    # Workers / people (ration tablets)
    "gurusz", "lu2", "geme2", "dumu", "ab-ba", "erin2",
    # Area / land measure
    "sar", "sar-ta", "iku",
    # Wool / fiber
    "siki", "siki-bi",
    # Food items
    "ninda", "ha-bu-um", "ma-sza-lum",
    # Offerings / ritual
    "nig2-ezem-ma",
    # Musical / ritual instruments
    "balag",
    # Transport
    "ma2",
    # Journey / expedition
    "kaskal",
    # Fraction markers
    "1/2(disz)", "1/3(disz)", "2/3(disz)", "5/6(asz)",
}

# Terms that mark non-animal tablet types when they follow numerals.
NON_ANIMAL_LINE_MARKERS = {
    "mu-kux(DU)", "mu-kux",           # "N deliveries" summary
    "siskur2",                         # ritual/offering formula
    "siskur2 a-sza3-ga",              # field offering
    "a-sza3-ga",                       # field/meadow (non-animal context)
    "nig2-dab5",                       # regular provisions (e2-u4-N)
    "sze-bi",                          # "its grain equivalent"
    "kusz",                            # leather/hide
    "kusze",                           # leather container
}

# Derived-commodity suppression: when one of these tokens appears *immediately*
# after a matched animal name (e.g. "gu4 siki"), the line records a product
# derived from that animal, NOT a live-animal count.  The entry is skipped.
DERIVED_COMMODITY_MARKERS = {
    "siki",       # wool (e.g. "gu4 siki" = wool of oxen)
    "siki-bi",    # its wool
    "kusz",       # hide / leather
    "kusze",      # leather container
}

# Carcass markers: appear *between* the numeral and the animal name.
# "3(disz) ad7 gu4" = 3 dead/carcass oxen.  Skip the marker and still count
# the animal; add qualifier "ba-usz2" (dead) automatically.
CARCASS_MARKERS = {"ad3", "ad6", "ad7"}

# Drehem (Reichskalender) month names → month number
MONTH_NAMES = {
    "masz-da3-gu7":            1,
    "ses-da-gu7":              2,
    "szesz-da-gu7":            2,
    "u5-bi2-gu7":              3,
    "ki-siki-{d}nin-a-zu":     4,
    "ezem-{d}nin-a-zu":        5,
    "a2-ki-ti":                6,
    "ezem-{d}szul-gi":         7,
    "szu-esz5-sza":            8,
    "ezem-mah":                9,
    "ezem-an-na":             10,
    "ezem-me-ki-gal2":        11,
    "sze-sag11-ku5":          12,
    "sze-il2-la":             12,  # variant
}

ANIMAL_TERMS = {
    # Cattle
    "gu4":           "ox",
    "gu4-gesz":      "draft ox",
    "gu4-niga":      "fattened ox",
    "ab2":           "cow",
    "amar":          "calf",
    "amar-gu4":      "bull calf",
    "amar ab2":      "heifer calf",
    # Sheep
    "udu":           "sheep",
    "udu-nita2":     "ram",
    "u8":            "ewe",
    "sila4":         "lamb",
    "kir11":         "female lamb",
    "gukkal":        "fat-tailed sheep",
    # Goats
    "masz2":         "goat",
    "masz2-gal":     "billy-goat",
    "ud5":           "nanny-goat",
    "{munus}asz2-gar3": "female kid",
    "dara4-nita2":   "male mountain goat",
    "dara4-munus":   "female mountain goat",
    "dara4":         "mountain goat",
    # Wild / exotic
    "masz-da3":      "gazelle",
    "masz-da3-nita2": "male gazelle",
    "masz-da3-munus": "female gazelle",
    "amar masz-da3": "gazelle calf",
    "lulim":         "red deer",
    "lulim-nita2":   "stag",
    "lulim-munus":   "female red deer/stag",
    "si-gar":        "wild sheep",
    "segbar":        "wild sheep",
    "szeg9-bar-nita2": "male deer",
    "szeg9-bar-munus": "female deer",
    "szeg9-bar nita2": "male deer",
    "szeg9-bar nita": "male deer",
    "szeg9-bar":     "deer",
    "az":            "bear",
    # Equids
    "ansze":         "equid",
    "ansze-kunga":   "mule",
    "{ansze}zi-zi-nita2": "stallion",
    "{ansze}zi-zi-munus": "mare",
    "{ansze}zi-zi":  "horse",
    "{ansze}si2-si2-nita2": "stallion",
    "{ansze}si2-si2-munus": "mare",
    "{ansze}si2-si2": "horse",
    "{ansze}kunga2-nita2": "male kunga-equid",
    "{ansze}kunga2-munus": "female kunga-equid",
    "{ansze}kunga2": "kunga-equid",
    "amar dur3":     "male donkey foal",
    "amar eme6":     "female donkey foal",
    "dur3":          "young male donkey",
    "eme6":          "female donkey",
    "dusu2-nita2":   "male donkey",
    "dusu2-munus":   "female donkey",
    "dusu2":         "donkey",
    # Pigs
    "szah2":         "pig",
    "szah2-NE-tur":  "piglet",
    "szah2-NE-tur-munus": "female piglet",
    "sza-munus":     "pig (female)",
    # Birds
    "tu-gur4{muszen}": "dove",
    "tu-gur4":       "dove",
    "ir7{muszen}":   "pigeon",
    "ir7":           "pigeon",
    "uz-tur":        "duckling",
    "uz-tur{muszen}": "duckling",
    "muszen":        "bird",
    "buru4{muszen}": "sparrow",
    "buru5{muszen}": "locust",
    "peszer{muszen}": "duck",
    # Wild cattle
    "am":            "wild bull",
    "am-si":         "elephant",
    # Wild / predators
    "pirig":         "lion",
    "pirig-tur":     "young lion",
    "ur-mah":        "lion",
    "ur-bar-ra":     "wolf",
    # Ibex / wild goat
    "dara3":         "ibex",
    "dara3-masz":    "ibex",
    # Equids
    "kunga2":        "equid",
    "kunga2-nita2":  "male equid",
    # Waterfowl
    "uz":            "goose",
    # Other
    "ur-gi7":        "dog",
    "ku6":           "fish",
    "udu-A.LUM":    "long-haired sheep",
    "|U8+HUL2|":    "sheep (composite sign)",
    "kun-gid2":      "tail-fat sheep",
}

# Terms that look like animals but aren't when in certain contexts.
# Key = animal term, value = list of contexts where it's NOT an animal.
ANIMAL_FALSE_POSITIVE_CONTEXTS = {
    "amar": ["amar-{d}", "{d}amar"],  # theophoric: Amar-Suen
    "masz2": ["masz2-e", "masz2-da-re-a"],  # extispicy, mashdarea offering
    "az": ["a-zu", "nin-a-zu", "ba-az", "-az-"],  # part of Ninazu etc.
    "sila4": ["sila4-a", "sila4 ga-"],  # only false when not preceded by numeral
    "u8": ["u8-a"],  # uncertain context
    "ir7": ["ir7-ra"],  # not pigeon when part of personal name
    "am": ["am-si", "{d}amar"],  # elephant, or Amar-Suen theophoric
    "uz": ["uz-ga", "e2-uz-ga"],  # e2-uzga (chamber), not goose
}

ANIMAL_QUALIFIERS = {
    # Feeding / quality
    "niga":      "fattened",
    "ga":        "suckling",
    "u2":        "grass-fed",
    "saga":      "fine",
    "sag":       "prime",
    # State
    "ba-usz2":   "dead",
    "ba-ug7":    "dead",
    "usz2":      "dead",
    "gub":       "standing/alive",
    "i3-ti-la":  "alive",
    "u3-tu-da":  "newborn",
    # Appearance
    "babbar":    "white",
    "babbar2":   "white",
    "gi6":       "black",
    "ge6":       "black",
    "su4":       "red/brown",
    "a-lum":     "long-haired",
    "bar-gal2":  "with fleece",
    "bar-su-ga": "shorn",
    "si":        "horned",
    # Sex
    "nita2":     "male",
    "nita":      "male",
    "munus":     "female",
    # Age / size
    "amar":      "young",
    "masz":      "twin",
    "nu2-a":     "mating/lying down",
    "gu4-e-us2-sa": "ox-fed",
    "szu-gid2":  "inspected",
    "kin-gi4-a": "returned/extispicy",
    "hi-a":      "various/mixed",
}

# Section-header terms: when a line has no numeral and consists of
# ANIMAL + one of these, it's a section label (e.g. "udu ba-usz2" = "dead sheep section"),
# NOT an implicit count=1 animal entry.
SECTION_HEADER_MARKERS = {"ba-usz2", "ba-ug7", "i3-ti-la"}

# Numeral parsing: ATF uses N(disz) = N, N(u) = N*10, N(gesz2) = N*60
# disz@t is a variant of disz (rotated sign) with same value
NUMERAL_PATTERN = re.compile(
    r"(\d+)\((disz(?:@t)?|u|gesz2|gesz'u|szar2|szar'u|szargal|asz)\)"
)

NUMERAL_MULTIPLIERS = {
    "disz":    1,
    "disz@t":  1,
    "asz":     1,
    "u":       10,
    "gesz2":   60,
    "gesz'u":  600,
    "szar2":   3600,
    "szar'u":  36000,
    "szargal": 216000,
}

# Pre-sorted animal terms (longest first) for unambiguous matching
SORTED_ANIMAL_TERMS = sorted(ANIMAL_TERMS.keys(), key=len, reverse=True)

# Admin keywords indicating a line is structural, not an animal entry
ADMIN_KEYWORDS = [
    "ki ", "giri3", "maszkim", "szu ba-ti", "i3-dab5",
    "ba-zi", "mu-kux", "iti ", "mu ", "kiszib3", "u4 ",
    "ugula ", "sipa ", "sa2-du11", "ensi2",
]

# Precompiled regex for detecting damaged numerals
DAMAGED_NUMERAL_PATTERN = re.compile(
    r"\[[\d()\w\s']*\d+\((?:disz(?:@t)?|u|gesz2|gesz'u|szar2|szar'u|szargal|asz)\)[\d()\w\s']*\]|"
    r"\d+\((?:disz(?:@t)?|u|gesz2|gesz'u|szar2|szar'u|szargal|asz)\)[#?]"
)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Person:
    name: str
    role: str              # source, receiver, intermediary, commissioner, sealer, deliverer
    title: str = ""        # official title if mentioned
    normalized_name: str = ""   # canonical name from ORACC (e.g. "Abbasaga")
    name_type: str = ""         # ORACC POS tag: PN, DN, GN, RN, etc.

@dataclass
class AnimalEntry:
    count: int
    animal: str
    qualifiers: list[str] = field(default_factory=list)
    raw: str = ""
    damaged: bool = False      # True if count involves damaged/uncertain readings
    is_imputed: bool = False   # True if count was deterministically inferred from totals
    imputed_count: int = 0     # the inferred value (0 if not imputed)

@dataclass
class DamageReport:
    total_signs: int = 0       # approximate total sign count
    damaged_signs: int = 0     # signs with # or ?
    broken_signs: int = 0      # signs inside [...]
    missing_lines: int = 0     # lines marked as broken/missing ($-lines)
    preservation: float = 1.0  # 0.0–1.0, computed from above

@dataclass
class TabletExtraction:
    tablet_id: str
    transaction_type: str = ""
    persons: list[Person] = field(default_factory=list)
    animals: list[AnimalEntry] = field(default_factory=list)
    total_animals: int = 0
    total_animals_certain: int = 0   # only from undamaged readings
    total_animals_uncertain: int = 0 # from damaged/reconstructed readings
    edge_total: int = 0              # scribe's total from left edge (0 = not found)
    has_summary_line: bool = False    # True if šu-nigin detected
    has_sza3_bi_ta: bool = False      # True if sza3-bi-ta sub-disbursement structure
    month: str = ""
    month_number: int = 0
    day: str = ""
    year: str = ""
    ruler: str = ""
    regnal_year: int = 0
    destination: str = ""
    destination_category: str = ""
    divine_recipients: list[str] = field(default_factory=list)
    source_office: str = ""
    receiver_office: str = ""
    damage: DamageReport = field(default_factory=DamageReport)
    # Deterministic imputation of damaged counts (lacunes resolved from totals)
    total_animals_resolved: int = 0   # post-imputation total; 0 with status=incomplete = NaN
    total_animals_status: str = "complete"   # complete | imputed | incomplete
    imputation_method: str = ""       # "" | "edge_total" | "szunigin"
    imputation_target: str = ""       # animal term that received the inferred count
    raw_lines: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def parse_numeral(text: str) -> int:
    """
    Convert ATF numeral notation to integer.

    Examples:
        '3(u) 2(disz)'           → 32
        '1(gesz2) la2 1(disz)'   → 59  (60 minus 1)
        '3(u) la2 1(disz)'       → 29  (30 minus 1)
    """
    # Handle 'la2' (minus) construction: split on 'la2' and subtract
    if "la2" in text:
        parts = re.split(r"\s+la2\s+", text, maxsplit=1)
        if len(parts) == 2:
            base = _sum_numerals(parts[0])
            subtract = _sum_numerals(parts[1])
            return max(0, base - subtract)

    return _sum_numerals(text)


def _sum_numerals(text: str) -> int:
    """Sum all ATF numeral tokens in a string."""
    total = 0
    for match in NUMERAL_PATTERN.finditer(text):
        value = int(match.group(1))
        multiplier = NUMERAL_MULTIPLIERS.get(match.group(2), 1)
        total += value * multiplier
    return total


def strip_atf_damage(text: str) -> str:
    """Remove damage markers (#, ?, !, [...], <<>>) but keep readable content.
    Also normalises ATF komma notation: s, → s (represents the sibilant ṣ;
    the comma is a transliteration convention, not a separator).
    """
    text = re.sub(r"<<[^>]*>>", "", text)        # <<...>> editorial corrections
    text = re.sub(r"<([^>]*)>", r"\1", text)     # <x> → x  (supplied signs)
    text = re.sub(r"\[([^\]]*)\]", r"\1", text)  # [x] → x  (reconstructed)
    text = text.replace("#", "").replace("?", "").replace("!", "")
    text = re.sub(r"\s+", " ", text)             # collapse double spaces from removal
    # We do NOT replace "s," with "s", to preserve ATF "s," for exact matches with dictionary keys (te-s,i-in-ma-ma)
    return text.strip()


def has_damage(text: str) -> bool:
    """Check if a line contains ATF damage markers (#, ?, [...])."""
    if "#" in text or "?" in text:
        return True
    if re.search(r"\[", text):
        return True
    return False


def assess_damage(transliteration: str) -> DamageReport:
    """Assess tablet preservation from ATF damage markers."""
    report = DamageReport()

    for raw_line in transliteration.split("\n"):
        line = raw_line.strip()

        # Count $ lines that indicate broken/missing content
        if line.startswith("$"):
            if any(kw in line.lower() for kw in ["broken", "missing", "traces"]):
                report.missing_lines += 1
            continue

        # Only count content lines
        if not re.match(r"^\d+[\.\']", line):
            continue

        content = re.sub(r"^\d+[\.\'] *", "", line)
        if not content:
            continue

        # Count signs inside [...] (broken/reconstructed)
        line_broken = 0
        brackets = re.findall(r"\[([^\]]*)\]", content)
        for bracketed in brackets:
            tokens = bracketed.split()
            n = len(tokens) if tokens else 1
            report.broken_signs += n
            line_broken += n

        # Remove bracket content for further counting
        remaining = re.sub(r"\[[^\]]*\]", "", content)

        # Count signs with # or ? (uncertain/damaged)
        tokens = remaining.split()
        for token in tokens:
            if "#" in token or "?" in token:
                report.damaged_signs += 1
            report.total_signs += 1

        # Add this line's broken signs to total
        report.total_signs += line_broken

    # Compute preservation ratio
    if report.total_signs > 0:
        bad = report.damaged_signs + report.broken_signs
        report.preservation = max(0.0, 1.0 - (bad / report.total_signs))
    else:
        report.preservation = 0.0

    # Penalize for missing lines/surfaces
    if report.missing_lines > 0:
        report.preservation *= max(0.3, 1.0 - (report.missing_lines * 0.15))

    report.preservation = round(report.preservation, 3)
    return report


def extract_content_lines(transliteration: str) -> list[str]:
    """Extract numbered content lines from ATF, skipping structural markers, seals, and left edge."""
    lines = []
    in_seal = False
    in_left = False

    for raw_line in transliteration.split("\n"):
        line = raw_line.strip()

        if line.startswith("@seal"):
            in_seal = True
            continue
        if in_seal:
            if line.startswith("@") and not line.startswith("@column"):
                in_seal = False
            else:
                continue

        # Skip @left edge section (contains scribe's total, not content)
        if line.lower() == "@left":
            in_left = True
            continue
        if in_left:
            if line.startswith("@") or line.startswith("&"):
                in_left = False
            else:
                continue

        if line.startswith(("@", "$", "#", "&")):
            continue

        # ATF continuation lines: start with '. ' (period + space)
        # These carry real content (e.g. `. iti diri ezem-{d}me-ki-gal2 ba-zal`)
        # but have no explicit line number, so the normal regex misses them.
        if re.match(r"^\.\s+", line):
            content = re.sub(r"^\.\s+", "", line).strip()
            if content:
                lines.append(content)
            continue

        if not line or not re.match(r"^\d+[\.']", line):
            continue

        # Strip line number prefix
        content = re.sub(r"^\d+[\.']\ *", "", line)
        if content:
            lines.append(content)

    return lines


# ---------------------------------------------------------------------------
# Transaction type extraction
# ---------------------------------------------------------------------------

def extract_transaction_type(lines: list[str]) -> str:
    """Identify primary transaction type from content lines."""
    # Primary transaction keywords, checked against the last lines first
    primary_keywords = {
        "szu ba-ti":    "receipt",
        "ba-zi":        "expenditure",
        "ba-an-zi":     "expenditure",   # n-infix variant, common in bala texts
        "i3-dab5":      "transfer",
        "mu-kux(DU)":   "delivery",
        "mu-kux(DU)-ra-ta": "delivery",
        "mu-kux(DU)-ra": "delivery",
        "mu-kux":       "delivery",
    }

    # Bug fix: damaged mu-kux detection.
    # strip_atf_damage removes bracket contents, so "mu-[kux(DU)...]" becomes
    # "mu-" and is never matched.  Pre-check on raw lines before stripping.
    _damaged_mukux = re.compile(r"mu-\[kux|mu-\[DU")
    for line in reversed(lines):
        if _damaged_mukux.search(line):
            full_text = " ".join(lines)
            if "mu-kux(DU) lugal" in full_text or "mu-kux lugal" in full_text:
                return "royal_delivery"
            return "delivery"

    # Scan lines in reverse: the last-occurring keyword is the primary transaction
    for line in reversed(lines):
        cleaned = strip_atf_damage(line)
        for kw, tx_type in primary_keywords.items():
            if kw in cleaned:
                if tx_type == "delivery":
                    full_text = " ".join(lines)
                    if "mu-kux(DU) lugal" in full_text or "mu-kux lugal" in full_text:
                        return "royal_delivery"
                return tx_type

    # Secondary patterns: document types that don't use the standard keywords
    full_text = " ".join(strip_atf_damage(l) for l in lines)
    if "sza3-bi-ta" in full_text:
        return "sub_disbursement"
    if "zi-ga" in full_text:
        return "expenditure"
    if "sa2-du11" in full_text:
        return "regular_offering"
    if "nig2-ka9" in full_text:
        return "running_account"
    if "la2-ia3" in full_text:
        return "deficit_account"
    if "u3-tu-da" in full_text:
        return "birth_record"

    return ""


# ---------------------------------------------------------------------------
# Person extraction
# ---------------------------------------------------------------------------

def strip_case_suffix(name: str) -> str:
    """Remove Sumerian case suffixes (-ta, -sze3, -ra, -ke4, etc.) from names."""
    suffixes = ["-ta", "-sze3", "-ra", "-ke4", "-ka", "-kam", "-me"]
    for suffix in suffixes:
        if name.endswith(suffix):
            return name[:-len(suffix)]
    return name


def extract_title_from_line(text: str) -> str:
    """Check if a line contains an official title after a person name."""
    for title in OFFICIAL_TITLES:
        if title in text:
            return title
    return ""


def extract_persons(lines: list[str]) -> list[Person]:
    """Extract persons and their administrative roles from tablet lines."""
    persons = []
    full_text = " || ".join(lines)

    for i, line in enumerate(lines):
        cleaned = strip_atf_damage(line)

        # ki PN-ta → source
        ki_match = re.search(r"ki\s+(.+?)-ta(?:\s|$)", cleaned)
        if ki_match:
            name = ki_match.group(1).strip()
            title = extract_title_from_line(cleaned)
            persons.append(Person(name=name, role="source", title=title))
        elif cleaned.startswith("ki ") and not ki_match:
            # ki PN (without -ta): source when preceded by zi-ga
            prev_cleaned = strip_atf_damage(lines[i - 1]).strip() if i > 0 else ""
            if prev_cleaned == "zi-ga" or "zi-ga" in prev_cleaned:
                ki_name_match = re.search(r"ki\s+(.+)", cleaned)
                if ki_name_match:
                    name = ki_name_match.group(1).strip()
                    title = extract_title_from_line(name)
                    if title:
                        name = name[:name.index(title)].strip()
                    if name and not any(name.startswith(kw) for kw in ["iti", "mu "]):
                        persons.append(Person(name=name, role="source", title=title))

        # PN szu ba-ti → receiver
        szu_match = re.search(r"^(.+?)\s+szu ba-ti", cleaned)
        if szu_match:
            name = szu_match.group(1).strip()
            persons.append(Person(name=name, role="receiver"))
        elif "szu ba-ti" in cleaned and i > 0:
            # Receiver might be on the previous line
            prev = strip_atf_damage(lines[i - 1])
            if not any(kw in prev for kw in ["ki ", "giri3", "maszkim", "iti ", "mu "]):
                name = prev.strip()
                title = extract_title_from_line(name)
                if title:
                    name = name.replace(title, "").strip()
                persons.append(Person(name=name, role="receiver", title=title))

        # PN i3-dab5 → receiver (same line or previous line)
        dab_match = re.search(r"^(.+?)\s+i3-dab5", cleaned)
        if dab_match:
            name = dab_match.group(1).strip()
            title = extract_title_from_line(name)
            if title:
                name = name[:name.index(title)].strip()
            persons.append(Person(name=name, role="receiver", title=title))
        elif re.match(r"^i3-dab5\s*$", cleaned) and i > 0:
            # i3-dab5 alone on line: receiver is on previous line
            prev = strip_atf_damage(lines[i - 1])
            if not any(kw in prev for kw in [
                "ki ", "giri3", "maszkim", "iti ", "mu ", "ba-zi",
                "szu ba-ti", "ba-usz2", "ba-ug7",
            ]):
                name = prev.strip()
                title = extract_title_from_line(name)
                if title:
                    name = name[:name.index(title)].strip()
                persons.append(Person(name=name, role="receiver", title=title))

        # giri3 PN [TITLE] → intermediary
        giri_match = re.search(r"giri3\s+(.+)", cleaned)
        if giri_match:
            rest = giri_match.group(1).strip()
            title = extract_title_from_line(rest)
            name = rest
            if title:
                name = rest[:rest.index(title)].strip()
            persons.append(Person(name=name, role="intermediary", title=title))

        # PN maszkim → commissioner
        maszkim_match = re.search(r"^(.+?)\s+maszkim", cleaned)
        if maszkim_match:
            name = maszkim_match.group(1).strip()
            # maszkim can follow a title
            title = extract_title_from_line(name)
            if title:
                name = name[:name.index(title)].strip()
            persons.append(Person(name=name, role="commissioner", title=title))

        # kiszib3 PN → sealer
        kiszib_match = re.search(r"kiszib3\s+(.+)", cleaned)
        if kiszib_match:
            name = kiszib_match.group(1).strip()
            persons.append(Person(name=name, role="sealer"))

        # mu-kux(DU) PN or mu-kux PN → deliverer (when person name follows)
        mukux_match = re.search(r"mu-kux(?:\(DU\))?\s+(.+)", cleaned)
        if mukux_match:
            name = mukux_match.group(1).strip()
            if not any(name.startswith(kw) for kw in ["lugal", "iti", "mu "]):
                # Strip title if present
                title = extract_title_from_line(name)
                if title:
                    name = name[:name.index(title)].strip()
                persons.append(Person(name=name, role="deliverer", title=title))

        # sza3-bi-ta marks sub-disbursement section (structural marker, not person)

        # zi-ga PN → expenditure agent (variant of ba-zi)
        # zi-ga bala ensi2 PLACE → bala-tax from governor of PLACE (not a person named "bala")
        ziga_match = re.search(r"zi-ga\s+(.+)", cleaned)
        if ziga_match:
            rest = ziga_match.group(1).strip()
            bala_gov = re.match(r"bala\s+ensi2\s+(.+)", rest)
            if bala_gov:
                place = bala_gov.group(1).strip()
                persons.append(Person(name=f"governor of {place}", role="source", title="ensi2"))
            else:
                name = rest
                title = extract_title_from_line(name)
                if title:
                    name = name[:name.index(title)].strip()
                if name and not any(name.startswith(kw) for kw in ["ki ", "iti", "mu ", "bala"]):
                    persons.append(Person(name=name, role="source", title=title))

        # bala PN ensi2 → official providing animals from rotating bala office
        bala_match = BALA_PATTERN.search(cleaned)
        if bala_match:
            name = bala_match.group("name").strip()
            title = "ensi2"  # governor — always accompanies bala entries
            persons.append(Person(
                name=name, role="source", title=title,
                name_type="bala-official",
            ))

    return persons


# ---------------------------------------------------------------------------
# Animal extraction
# ---------------------------------------------------------------------------

def is_summary_line(line: str) -> bool:
    """Check if a line is a šu-nigin (total) or nigin2-ba (altogether) summary line."""
    cleaned = strip_atf_damage(line)
    return bool(re.search(r"szu-nigin2?|szu-nigin\b|szunigin|nigin2-ba", cleaned))


def is_szunigin_line(line: str) -> bool:
    """Check if a line is specifically a šu-nigin (category total), not nigin2-ba."""
    cleaned = strip_atf_damage(line)
    return bool(re.search(r"szu-nigin2?|szu-nigin\b|szunigin", cleaned))


def is_niginba_line(line: str) -> bool:
    """Check if a line is a nigin2-ba (aggregate altogether) line."""
    cleaned = strip_atf_damage(line)
    return bool(re.search(r"nigin2-ba", cleaned))


def find_sza3_bi_ta_split(lines: list[str]) -> int:
    """Find index of sza3-bi-ta marker line. Returns -1 if not found."""
    for i, line in enumerate(lines):
        cleaned = strip_atf_damage(line)
        if "sza3-bi-ta" in cleaned:
            return i
    return -1


def _is_animal_false_positive(animal: str, line: str) -> bool:
    """
    Check if an animal term match is actually a false positive,
    e.g. part of a personal name, place name, or month name.
    """
    # "az" is very short and appears in many non-animal contexts
    if animal == "az":
        # Only count as bear if it's a standalone word
        words = line.split()
        if animal not in words:
            return True
        idx = words.index(animal)
        # If preceded by a case marker or part of compound, skip
        if idx > 0 and words[idx - 1].endswith(("-a", "-ba")):
            return True

    # "ir7" can appear in personal names
    if animal in ("ir7", "ir7{muszen}"):
        if "ir7-ra" in line or "ir7-mu" in line or "ir7-da" in line:
            return True

    # "u8" is very short; skip if inside a compound
    if animal == "u8":
        idx = line.find("u8")
        if idx > 0 and line[idx - 1] not in (" ", "\t"):
            return True

    return False


def _find_implicit_animals(
    line: str,
    raw_line: str,
    sorted_animals: list[str],
) -> list[AnimalEntry]:
    """
    Find animal terms on lines with no ATF numeral prefix.
    These represent implicit count=1.

    E.g. "gu4 ki ur-{d}ba-ba6 dub-sar-ta" → 1 ox

    BUT lines like "udu ba-usz2" or "i3-ti-la" are section headers
    (state labels for preceding animals), NOT implicit count=1 entries.
    """
    cleaned = strip_atf_damage(line)
    words = cleaned.split()
    if not words:
        return []

    # Skip standalone section-header lines: "ANIMAL ba-usz2" / "ANIMAL i3-ti-la"
    # These label the state of preceding animals, not a new animal entry.
    remaining_after_first = " ".join(words[1:]) if len(words) > 1 else ""
    for marker in SECTION_HEADER_MARKERS:
        if remaining_after_first == marker or cleaned == marker:
            return []

    entries = []
    for animal in sorted_animals:
        if cleaned.startswith(animal):
            end_pos = len(animal)
            if end_pos >= len(cleaned) or cleaned[end_pos] in " \t":
                if _is_animal_false_positive(animal, cleaned):
                    continue
                remaining = cleaned[end_pos:].strip()
                remaining_words = remaining.split() if remaining else []

                # Also skip if the only remaining content is a section marker
                if remaining in SECTION_HEADER_MARKERS:
                    return []

                qualifiers = []
                for qual in ANIMAL_QUALIFIERS:
                    if qual in remaining_words:
                        qualifiers.append(qual)
                entries.append(AnimalEntry(
                    count=1,
                    animal=animal,
                    qualifiers=qualifiers,
                    raw=cleaned.strip(),
                    damaged=has_damage(raw_line),
                ))
                break
    return entries


def extract_animals(lines: list[str]) -> tuple[list[AnimalEntry], bool, bool]:
    """Extract animal entries with counts and qualifiers. Returns (entries, has_summary, has_sza3_bi_ta)."""
    sorted_animals = SORTED_ANIMAL_TERMS

    # Prefer szunigin over nigin2-ba to avoid double-counting.
    szunigin_indices = [i for i, line in enumerate(lines) if is_szunigin_line(line)]
    niginba_indices = [i for i, line in enumerate(lines) if is_niginba_line(line)]

    if szunigin_indices:
        summary_indices = szunigin_indices
    elif niginba_indices:
        summary_indices = niginba_indices
    else:
        summary_indices = []

    has_summary = len(summary_indices) > 0
    has_sza3 = find_sza3_bi_ta_split(lines) >= 0

    entries = []

    for idx, line in enumerate(lines):
        raw_line = line
        cleaned = strip_atf_damage(line)

        if has_summary:
            if idx not in summary_indices:
                continue

        is_admin = any(kw in cleaned for kw in ADMIN_KEYWORDS)
        has_nums = NUMERAL_PATTERN.search(cleaned)

        # Check if line starts with an animal term (potential implicit count=1)
        starts_with_animal = False
        if not has_nums:
            for animal in sorted_animals:
                if cleaned.startswith(animal):
                    end_pos = len(animal)
                    if end_pos >= len(cleaned) or cleaned[end_pos] in " \t":
                        starts_with_animal = True
                        break

        # Skip pure admin lines (unless they start with an animal term)
        if is_admin and not has_nums and not starts_with_animal:
            continue

        line_has_damage = has_damage(raw_line)
        numeral_damaged = bool(DAMAGED_NUMERAL_PATTERN.search(raw_line))

        if has_nums:
            # Standard extraction: numeral + animal
            pos = 0
            while pos < len(cleaned):
                num_match = NUMERAL_PATTERN.search(cleaned, pos)
                if not num_match:
                    break

                num_start = num_match.start()
                num_end = num_match.end()
                while True:
                    rest = cleaned[num_end:]
                    ext = re.match(
                        r"\s+(?:la2\s+)?(\d+\((?:disz(?:@t)?|u|gesz2|gesz'u|szar2|szar'u|szargal|asz)\))",
                        rest,
                    )
                    if ext:
                        num_end += ext.end()
                    else:
                        break

                numeral_text = cleaned[num_start:num_end]
                after_num = cleaned[num_end:].strip()

                # Bug fix: carcass-marker handling.
                # "N ad7 ANIMAL" → skip ad7/ad6/ad3, match animal, add qualifier ba-usz2
                after_for_animal = after_num
                carcass_qualifier = None
                for _cm in CARCASS_MARKERS:
                    if after_for_animal.startswith(_cm + " ") or after_for_animal == _cm:
                        carcass_qualifier = "ba-usz2"
                        after_for_animal = after_for_animal[len(_cm):].strip()
                        break

                animal_found = None
                for animal in sorted_animals:
                    if after_for_animal.startswith(animal):
                        end_pos = len(animal)
                        if end_pos >= len(after_for_animal) or after_for_animal[end_pos] in " \t":
                            if not _is_animal_false_positive(animal, after_for_animal):
                                animal_found = animal
                            break

                if animal_found:
                    count = parse_numeral(numeral_text)
                    remaining = after_for_animal[len(animal_found):].strip()
                    remaining_words = re.split(r"[\s]+", remaining)

                    # Bug fix: derived-commodity suppression.
                    # "N ANIMAL siki" = wool/hide derived from the animal, not a live animal.
                    # Skip the entire entry; the line is a commodity record, not an animal record.
                    first_remaining = remaining_words[0] if remaining_words and remaining_words[0] else ""
                    if first_remaining in DERIVED_COMMODITY_MARKERS:
                        pos = num_end + len(animal_found) + 1
                        continue

                    qualifiers = []
                    if carcass_qualifier:
                        qualifiers.append(carcass_qualifier)
                    for qual in ANIMAL_QUALIFIERS:
                        if qual in remaining_words:
                            qualifiers.append(qual)

                    entries.append(AnimalEntry(
                        count=count,
                        animal=animal_found,
                        qualifiers=qualifiers,
                        raw=cleaned.strip(),
                        damaged=numeral_damaged or (line_has_damage and bool(re.search(
                            r"[\[#?].*?" + re.escape(str(count)),
                            raw_line
                        ))),
                    ))
                    pos = num_end + len(animal_found) + 1
                else:
                    pos = num_end + 1
        elif not has_summary:
            # Implicit count=1: line starts with animal term, no numeral
            implicit = _find_implicit_animals(line, raw_line, sorted_animals)
            entries.extend(implicit)

    # [x] ANIMAL [...] — broken numeral with identifiable animal. Match on raw line before stripping damage.
    for raw_line in lines:
        for x_match in re.finditer(r"\[x\]\s+", raw_line):
            if is_summary_line(raw_line):
                continue
            # Strip damage for the animal lookup: "[x] udu [...]" → "udu"
            after_bracket = raw_line[x_match.end():]
            after_x = strip_atf_damage(after_bracket).strip()
            for animal in sorted_animals:
                if not after_x.startswith(animal):
                    continue
                end_pos = len(animal)
                if end_pos < len(after_x) and after_x[end_pos] not in " \t":
                    continue
                if _is_animal_false_positive(animal, after_x):
                    break
                qualifiers = []
                remaining = after_x[len(animal):].strip()
                remaining_words = re.split(r"[\s]+", remaining)
                for qual in ANIMAL_QUALIFIERS:
                    if qual in remaining_words:
                        qualifiers.append(qual)

                entries.append(AnimalEntry(
                    count=1,  # placeholder; real value imputed later if possible
                    animal=animal,
                    qualifiers=qualifiers,
                    raw=raw_line.strip(),
                    damaged=True,
                ))
                break

    return entries, has_summary, has_sza3


# ---------------------------------------------------------------------------
# Date extraction
# ---------------------------------------------------------------------------

def extract_date(lines: list[str]) -> tuple[str, str, str]:
    """Extract month (iti), day (u4), and year (mu). Returns (month, day, year)."""
    month = ""
    day = ""
    year = ""

    for line in lines:
        cleaned = strip_atf_damage(line)

        # Month: iti MONTH-NAME  (also handles "iti diri MONTH-NAME")
        iti_match = re.search(r"iti\s+(.+?)(?:\s*$)", cleaned)
        if iti_match and not month:
            candidate = iti_match.group(1).strip()
            # Don't capture "iti u4 ..." as month — that's a day-within-month formula
            if not candidate.startswith("u4"):
                # Strip trailing day-formula: "... ba-zal"
                candidate = re.sub(r"\s+ba-zal.*$", "", candidate).strip()
                month = candidate

        # Day: u4 N-kam or u4 N ba-zal  (la2 subtraction handled by parse_numeral,
        # so "u4 3(u) la2 1" correctly yields day 29)
        day_match = re.search(r"u4\s+([\d()\w\s]+?)(?:-kam|\s+ba-zal|\s+ba-ra-zal)", cleaned)
        if day_match and not day:
            day_num = parse_numeral(day_match.group(1))
            if day_num > 0:
                day = str(day_num)

        # iti-ta u4 N ba-ra-zal: "of the month, day N has passed" (P123620)
        iti_ta_match = re.search(r"iti-ta\s+u4\s+([\d()\w\s]+?)\s+ba-ra-zal", cleaned)
        if iti_ta_match and not day:
            day_num = parse_numeral(iti_ta_match.group(1))
            if day_num > 0:
                day = str(day_num)

        # Year: mu ... (year name, usually on its own line)
        if re.match(r"^mu\s+", cleaned) and not year:
            year = cleaned

        # Also catch "iti u4 N ba-zal" combined pattern
        iti_day_match = re.search(r"iti\s+u4\s+([\d()\w\s]+?)ba-zal", cleaned)
        if iti_day_match and not day:
            day_num = parse_numeral(iti_day_match.group(1))
            if day_num > 0:
                day = str(day_num)

    return month, day, year


# ---------------------------------------------------------------------------
# Destination extraction
# ---------------------------------------------------------------------------

def extract_destination(lines: list[str]) -> tuple[str, str]:
    """
    Extract destination and its category.
    Returns (raw_destination, category).
    """
    for line in lines:
        cleaned = strip_atf_damage(line)
        dest_match = re.search(r"([\w{}-]+)-sze3", cleaned)
        if dest_match:
            dest = dest_match.group(1)
            if dest not in ["kur mar-tu"]:
                raw = dest + "-sze3"
                category = DESTINATION_CATEGORIES.get(dest, "")
                return raw, category

        # "sza3 PLACE" = "in PLACE" (location marker)
        sza3_match = re.search(r"sza3\s+([\w{}-]+)", cleaned)
        if sza3_match:
            place = sza3_match.group(1).strip()
            category = DESTINATION_CATEGORIES.get(place, "")
            if category:
                return f"sza3 {place}", category

        # "DEST ba-an-kux" = "brought into DEST" (palace delivery etc.)
        kux_match = re.search(r"([\w{}-]+)\s+ba-an-kux", cleaned)
        if kux_match:
            dest = kux_match.group(1).strip()
            # Strip common suffixes like -la
            dest_base = re.sub(r"-la$", "", dest)
            category = DESTINATION_CATEGORIES.get(dest, "") or DESTINATION_CATEGORIES.get(dest_base, "")
            if category:
                return f"{dest} ba-an-kux", category
    return "", ""


# ---------------------------------------------------------------------------
# Divine recipient extraction
# ---------------------------------------------------------------------------

def _is_standalone_deity(deity: str, line: str) -> bool:
    """Check if deity appears standalone, not as part of a personal name (theophoric)."""
    idx = 0
    while True:
        idx = line.find(deity, idx)
        if idx < 0:
            return False

        before = line[:idx]
        after = line[idx + len(deity):]

        # Check if preceded by name-building element (theophoric prefix)
        before_ok = (idx == 0 or before[-1] in (" ", "\t"))
        # Allow "ezem-{d}" and "e2-{d}" as valid deity contexts (festivals, temples)
        if not before_ok and before.endswith(("ezem-", "e2-", "sza3-")):
            before_ok = True

        # Check if followed by name-building suffix (theophoric suffix)
        after_ok = (len(after) == 0 or after[0] in (" ", "\t"))
        # Also ok if followed by case suffixes like -sze3, -ra, -ke4
        if not after_ok:
            for suffix in ["-sze3", "-ra", "-ke4", "-kam", "-ta", "-me", "-e-ne"]:
                if after.startswith(suffix):
                    rest_after_suffix = after[len(suffix):]
                    if not rest_after_suffix or rest_after_suffix[0] in (" ", "\t"):
                        after_ok = True
                        break

        theophoric_prefixes = (
            "ur-", "lu2-", "geme2-", "ARAD2-", "szu-",
            "puzur4-", "nu-ur2-", "dingir-", "a-hu-", "dan-",
        )
        if before.endswith(theophoric_prefixes):
            before_ok = False

        if before_ok and after_ok:
            return True

        idx += 1

    return False


def extract_divine_recipients(
    lines: list[str],
    extracted_persons: list[Person] | None = None,
) -> list[str]:
    """Identify deities appearing as recipients in offering/expenditure texts."""
    person_names = set()
    if extracted_persons:
        for p in extracted_persons:
            person_names.add(p.name)

    recipients = []
    for line in lines:
        cleaned = strip_atf_damage(line)

        # Skip lines with person-role markers
        if any(kw in cleaned for kw in [
            "ki ", "giri3 ", "kiszib3", "mu-kux", "maszkim",
            "szu ba-ti", "i3-dab5", "ba-zi", "ba-an-zi", "zi-ga",
        ]):
            continue

        # Skip year name and month name lines
        if re.match(r"^(mu|iti)\s+", cleaned):
            continue

        for deity in KNOWN_DEITIES:
            if deity in cleaned and deity not in person_names:
                if _is_standalone_deity(deity, cleaned):
                    recipients.append(deity)

    return list(dict.fromkeys(recipients))


# ---------------------------------------------------------------------------
# Office classification
# ---------------------------------------------------------------------------

def classify_office(persons: list[Person]) -> tuple[str, str]:
    """
    Map known officials to their office branch.
    Returns (source_office, receiver_office) as "; "-joined strings when
    multiple distinct offices appear on the same side (rare but real).

    Roles considered:
      source side  → role in ("source", "deliverer")
      receiver side → role in ("receiver", "sealer")

    Intermediaries and commissioners are not used for office assignment —
    they supervise but do not own the transaction.
    """
    source_offices: list[str] = []
    receiver_offices: list[str] = []
    seen_source: set[str] = set()
    seen_receiver: set[str] = set()

    for person in persons:
        office_info = OFFICIALS_TO_OFFICE.get(person.name)
        if not office_info:
            continue
        _, branch = office_info

        if person.role in ("source", "deliverer"):
            if branch not in seen_source:
                source_offices.append(branch)
                seen_source.add(branch)
        elif person.role in ("receiver", "sealer"):
            if branch not in seen_receiver:
                receiver_offices.append(branch)
                seen_receiver.add(branch)

    return "; ".join(source_offices), "; ".join(receiver_offices)


# ---------------------------------------------------------------------------
# ORACC name normalization
# ---------------------------------------------------------------------------

def normalize_persons(persons: list[Person]) -> None:
    """Enrich each Person with ORACC canonical name if available."""
    if not _HAS_NAME_DICT:
        return

    for person in persons:
        result = _oracc_normalize(person.name)
        if result:
            person.normalized_name = result["canonical"]
            person.name_type = result["pos"]


# ---------------------------------------------------------------------------
# Structured date from CDLI date_of_origin column
# ---------------------------------------------------------------------------

def parse_date_of_origin(date_str: str) -> tuple[str, int, int]:
    """Parse CDLI date_of_origin column. Returns (ruler, regnal_year, month_number)."""
    if not date_str or date_str in ("00.00.00.00", "--.--.--.--"):
        return "", 0, 0

    # Strip annotations like "(us2 year)", "(intercalated)"
    clean = re.sub(r"\s*\(.*?\)", "", date_str).strip()
    parts = clean.split(".")

    if len(parts) < 2:
        return "", 0, 0

    ruler = parts[0]

    try:
        regnal_year = int(parts[1])
    except ValueError:
        regnal_year = 0

    month_num = 0
    if len(parts) >= 3:
        # Handle intercalary months like "12d"
        month_str = re.sub(r"[a-zA-Z]", "", parts[2])
        try:
            month_num = int(month_str)
        except ValueError:
            month_num = 0

    return ruler, regnal_year, month_num


def apply_imputation(extraction: TabletExtraction) -> None:
    """Deterministic imputation of damaged animal counts (lacunes).

    Rationale: Ur III administrative tablets are self-balancing. When a
    body line has a missing count [x] but the scribal grand total is intact,
    the missing value can be derived as T - sum(known).

    Sets these fields on the extraction:
        total_animals_resolved : int   – sum after imputation; 0 if still NaN
        total_animals_status   : str   – "complete" | "imputed" | "incomplete"
        imputation_method      : str   – "" | "edge_total" | "szunigin"
        imputation_target      : str   – animal term that received the value

    Conditions for imputation:
        – Exactly one damaged AnimalEntry on the tablet.
        – A trustworthy reference total (edge_total preferred) is present.
        – Reference total – sum(certain entries) ≥ 1.

    Otherwise the record is flagged as "incomplete" so downstream summation
    code can treat it as NaN (listwise deletion), while network-analysis
    code can still consult source/receiver edges.
    """
    extraction.total_animals_resolved = extraction.total_animals
    extraction.total_animals_status = "complete"
    extraction.imputation_method = ""
    extraction.imputation_target = ""

    damaged = [a for a in extraction.animals if a.damaged]
    if not damaged:
        return

    # When entries originate from the szu-nigin summary, the body counts are
    # not individually extracted; imputing within the summary itself is not
    # meaningful, so we only flag completeness.
    if extraction.has_summary_line:
        if any(a.damaged for a in extraction.animals):
            extraction.total_animals_status = "incomplete"
            extraction.total_animals_resolved = 0
        return

    if len(damaged) != 1:
        extraction.total_animals_status = "incomplete"
        extraction.total_animals_resolved = 0
        return

    reference = extraction.edge_total
    if reference <= 0:
        extraction.total_animals_status = "incomplete"
        extraction.total_animals_resolved = 0
        return

    sum_certain = sum(a.count for a in extraction.animals if not a.damaged)
    inferred = reference - sum_certain
    if inferred < 1:
        # Either the reference is wrong or there are extra unrecorded entries.
        # Refuse to fabricate a value.
        extraction.total_animals_status = "incomplete"
        extraction.total_animals_resolved = 0
        return

    target = damaged[0]
    target.imputed_count = inferred
    target.is_imputed = True
    extraction.imputation_method = "edge_total"
    extraction.imputation_target = target.animal
    extraction.total_animals_resolved = sum_certain + inferred
    extraction.total_animals_status = "imputed"


def _extract_edge_total(transliteration: str) -> int:
    """Extract scribe's total from the @left edge section of the tablet.

    In ATF, the left edge is marked by an '@left' structural line.
    Content lines after @left typically contain just a numeral = the scribe's
    running total of animals on the tablet.
    Falls back to scanning last 3 content lines only if no @left section exists.
    """
    # Strategy 1: Look for @left section in raw ATF
    in_left = False
    left_lines = []
    for raw_line in transliteration.split("\n"):
        line = raw_line.strip()
        if line.lower() == "@left":
            in_left = True
            continue
        if in_left:
            if line.startswith("@") or line.startswith("&"):
                break
            if re.match(r"^\d+[\.']", line):
                content = re.sub(r"^\d+[\.']\ *", "", line)
                if content:
                    left_lines.append(content)

    if left_lines:
        for line in left_lines:
            cleaned = strip_atf_damage(line)
            tokens = cleaned.split()
            if not tokens:
                continue
            # Pure numeral line = edge total
            all_numeral = all(
                NUMERAL_PATTERN.match(t) or t == "la2" for t in tokens
            )
            if all_numeral:
                val = parse_numeral(cleaned)
                if val > 0:
                    return val
        # @left exists but no pure numeral found (e.g. date on edge)
        return 0

    # Strategy 2: No @left section. Do NOT guess from content lines.
    # False positives from standalone numerals in content are too common.
    return 0


# ---------------------------------------------------------------------------
# Main extraction pipeline
# ---------------------------------------------------------------------------

def extract_tablet(
    tablet_id: str,
    transliteration: str,
    date_of_origin: str = "",
) -> TabletExtraction:
    """Run full extraction pipeline on a single tablet transliteration."""
    lines = extract_content_lines(transliteration)

    result = TabletExtraction(
        tablet_id=tablet_id,
        raw_lines=lines,
    )

    if not lines:
        # Even without transliteration, parse the CDLI date
        result.ruler, result.regnal_year, result.month_number = parse_date_of_origin(date_of_origin)
        return result

    result.transaction_type = extract_transaction_type(lines)
    result.persons = extract_persons(lines)
    normalize_persons(result.persons)
    result.animals, result.has_summary_line, result.has_sza3_bi_ta = extract_animals(lines)
    result.total_animals = sum(a.count for a in result.animals)
    result.total_animals_certain = sum(a.count for a in result.animals if not a.damaged)
    result.total_animals_uncertain = sum(a.count for a in result.animals if a.damaged)
    result.edge_total = _extract_edge_total(transliteration)
    result.month, result.day, result.year = extract_date(lines)
    result.destination, result.destination_category = extract_destination(lines)
    result.divine_recipients = extract_divine_recipients(lines, result.persons)
    result.source_office, result.receiver_office = classify_office(result.persons)
    result.damage = assess_damage(transliteration)

    # Calendar: use CDLI date_of_origin as primary, fall back to extracted month
    result.ruler, result.regnal_year, result.month_number = parse_date_of_origin(date_of_origin)
    if result.month_number == 0 and result.month:
        result.month_number = MONTH_NAMES.get(result.month, 0)

    # Deterministic imputation of damaged animal counts from edge totals.
    # Must run after edge_total and animals are populated.
    apply_imputation(result)

    return result


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------

def process_database(
    input_path: Path,
    output_path: Path,
    limit: int = 0,
) -> None:
    """Process input CSV and produce enriched output with extracted fields."""
    with open(input_path, "r", encoding="utf-8") as infile, \
         open(output_path, "w", encoding="utf-8", newline="") as outfile:

        reader = csv.DictReader(infile)

        out_fields = [
            "tablet_id", "designation", "date_of_origin",
            "ruler", "regnal_year", "month_number", "day",
            "transaction_type",
            "source", "receiver", "intermediary", "commissioner",
            "sealer", "deliverer",
            "source_normalized", "receiver_normalized",
            "intermediary_normalized", "commissioner_normalized",
            "source_title", "receiver_title", "intermediary_title",
            "source_office", "receiver_office",
            "animals_detail", "total_animals",
            "total_animals_certain", "total_animals_uncertain",
            "total_animals_resolved", "total_animals_status",
            "imputation_method", "imputation_target",
            "edge_total",
            "has_summary_line", "has_sza3_bi_ta",
            "divine_recipients", "destination", "destination_category",
            "month_name", "year_name",
            "has_transliteration", "extraction_score",
            "preservation_score", "damaged_signs", "broken_signs",
        ]
        writer = csv.DictWriter(outfile, fieldnames=out_fields)
        writer.writeheader()

        count = 0
        for row in reader:
            trans = row.get("transliteration", "").strip()
            tid = row.get("id_text", "")
            designation = row.get("designation", "")
            date_origin = row.get("date_of_origin", "")

            extraction = extract_tablet(tid, trans, date_of_origin=date_origin)

            # Organize persons by role
            persons_by_role = {}
            normalized_by_role = {}
            titles_by_role = {}
            for p in extraction.persons:
                if p.role not in persons_by_role:
                    persons_by_role[p.role] = []
                    normalized_by_role[p.role] = []
                    titles_by_role[p.role] = []
                persons_by_role[p.role].append(p.name)
                normalized_by_role[p.role].append(
                    p.normalized_name if p.normalized_name else p.name
                )
                if p.title:
                    titles_by_role[p.role].append(p.title)

            # Format animals as compact string: "3×udu-niga, 1×sila4-ga".
            # An imputed entry is rendered as "[imp:7]×animal" so the source
            # of the count remains visible in the CSV.
            animal_parts = []
            for a in extraction.animals:
                quals = "-".join(a.qualifiers) if a.qualifiers else ""
                if a.is_imputed:
                    label = f"[imp:{a.imputed_count}]×{a.animal}"
                else:
                    label = f"{a.count}×{a.animal}"
                if quals:
                    label += f"-{quals}"
                animal_parts.append(label)

            extraction_score = compute_extraction_score(extraction, bool(trans))

            writer.writerow({
                "tablet_id": tid,
                "designation": designation,
                "date_of_origin": date_origin,
                "ruler": extraction.ruler,
                "regnal_year": extraction.regnal_year or "",
                "month_number": extraction.month_number or "",
                "day": extraction.day,
                "transaction_type": extraction.transaction_type,
                "source": "; ".join(persons_by_role.get("source", [])),
                "receiver": "; ".join(persons_by_role.get("receiver", [])),
                "intermediary": "; ".join(persons_by_role.get("intermediary", [])),
                "commissioner": "; ".join(persons_by_role.get("commissioner", [])),
                "sealer": "; ".join(persons_by_role.get("sealer", [])),
                "deliverer": "; ".join(persons_by_role.get("deliverer", [])),
                "source_normalized": "; ".join(normalized_by_role.get("source", [])),
                "receiver_normalized": "; ".join(normalized_by_role.get("receiver", [])),
                "intermediary_normalized": "; ".join(normalized_by_role.get("intermediary", [])),
                "commissioner_normalized": "; ".join(normalized_by_role.get("commissioner", [])),
                "source_title": "; ".join(titles_by_role.get("source", [])),
                "receiver_title": "; ".join(titles_by_role.get("receiver", [])),
                "intermediary_title": "; ".join(titles_by_role.get("intermediary", [])),
                "source_office": extraction.source_office,
                "receiver_office": extraction.receiver_office,
                "animals_detail": ", ".join(animal_parts),
                "total_animals": extraction.total_animals,
                "total_animals_certain": extraction.total_animals_certain,
                "total_animals_uncertain": extraction.total_animals_uncertain,
                # NaN encoding: explicit string for tablets that could not be resolved.
                # Listwise-deletion code can drop rows where this field == "NaN".
                "total_animals_resolved": (
                    "NaN" if extraction.total_animals_status == "incomplete"
                    else extraction.total_animals_resolved
                ),
                "total_animals_status": extraction.total_animals_status,
                "imputation_method": extraction.imputation_method,
                "imputation_target": extraction.imputation_target,
                "edge_total": extraction.edge_total or "",
                "has_summary_line": "1" if extraction.has_summary_line else "0",
                "has_sza3_bi_ta": "1" if extraction.has_sza3_bi_ta else "0",
                "divine_recipients": "; ".join(extraction.divine_recipients),
                "destination": extraction.destination,
                "destination_category": extraction.destination_category,
                "month_name": extraction.month,
                "year_name": extraction.year,
                "has_transliteration": "1" if trans else "0",
                "extraction_score": extraction_score,
                "preservation_score": extraction.damage.preservation,
                "damaged_signs": extraction.damage.damaged_signs,
                "broken_signs": extraction.damage.broken_signs,
            })

            count += 1
            if limit and count >= limit:
                break

        print(f"Processed {count} tablets → {output_path}")


def compute_extraction_score(extraction: TabletExtraction, has_trans: bool) -> str:
    """
    Assign a data-quality label (high/medium/low/none) based on how complete
    and well-preserved a tablet's extraction is.

    This is NOT an accuracy score — it measures tablet legibility and field
    coverage, not whether the extracted values are correct.  Use it to filter
    tablets before analysis (e.g. drop 'low' and 'none').
    """
    if not has_trans:
        return "none"

    # Completeness (0–4 points)
    completeness = sum([
        bool(extraction.transaction_type),
        bool(extraction.persons),
        bool(extraction.animals),
        bool(extraction.month or extraction.year),
    ])

    # Preservation factor (0.0–1.0)
    preservation = extraction.damage.preservation

    # Penalty for uncertain animal counts
    uncertain_ratio = 0.0
    if extraction.total_animals > 0:
        uncertain_ratio = extraction.total_animals_uncertain / extraction.total_animals

    # Combined score: completeness weighted by preservation
    # Max = 4.0 (all fields extracted, perfect preservation)
    score = completeness * preservation

    # Extra penalty if most animal counts are uncertain
    if uncertain_ratio > 0.5:
        score *= 0.8

    if score >= 3.0:
        return "high"
    elif score >= 1.5:
        return "medium"
    elif score > 0:
        return "low"
    return "none"


# ---------------------------------------------------------------------------
# Diagnostic issue detection
# ---------------------------------------------------------------------------

def detect_issues(
    tablet_id: str,
    transliteration: str,
    extraction: TabletExtraction,
) -> list[dict]:
    """Detect potential extraction issues in a single tablet."""
    issues = []
    lines = extract_content_lines(transliteration)

    # Numeral lines with no recognized animal
    for line in lines:
        cleaned = strip_atf_damage(line)
        if not NUMERAL_PATTERN.search(cleaned):
            continue
        if any(animal in cleaned for animal in SORTED_ANIMAL_TERMS):
            continue
        # Date/structural lines
        if any(kw in cleaned for kw in ["u4 ", "-kam", "ba-zal", "iti ", "mu "]):
            continue
        # Standalone numeral lines (subtotals, edge totals)
        stripped = cleaned.strip().replace(" ", "").replace("(", "").replace(")", "")
        for t in ("disz", "disz@t", "u", "gesz2", "gesz'u", "szar2", "szar'u", "szargal", "asz", "la2"):
            stripped = stripped.replace(t, "")
        if stripped.isdigit() or stripped == "":
            continue
        # Broken/unreadable text: if line is mostly damage markers, skip
        all_words = cleaned.split()
        damage_tokens = {"x", "...", "n", "xx", "X", "&"}
        non_damage = [w for w in all_words
                      if w not in damage_tokens
                      and not NUMERAL_PATTERN.match(w)
                      and w != "la2"
                      and w != "."
                      and not re.match(r"^\d+/\d+\(", w)]
        # Skip if all content is damaged/unreadable
        if not non_damage or all(w in damage_tokens for w in non_damage):
            continue
        # If more than half the tokens are damage markers, downgrade to info
        total_tokens = len(all_words)
        damage_count = sum(1 for w in all_words if w in damage_tokens or w == ".")
        heavily_damaged = damage_count > total_tokens / 2
        # Check measure/commodity terms anywhere in the line
        if any(term in all_words for term in MEASURE_TERMS):
            continue
        # Compound measure terms: zi3-ta, kasz-ta, zi3-gu, etc.
        if any(w.split("-")[0] in MEASURE_TERMS for w in all_words if "-" in w):
            continue
        # Grain ration notation: lines with (barig) or (ban2) volume numerals
        if re.search(r"\d+\((?:barig|ban2)\)", cleaned):
            continue
        # Check non-animal line markers
        if any(marker in cleaned for marker in NON_ANIMAL_LINE_MARKERS):
            continue
        # Blank-space subtotal lines
        if "blank space" in line:
            continue
        # e2-u4-N patterns (building/temple names with embedded numerals)
        if re.search(r"e2-u4-\d+", cleaned):
            continue
        # Wooden objects: gesz + object name
        if any(w.startswith("gesz") or w.startswith("{gesz}") for w in all_words):
            continue
        # Geographic qualifiers with no animal
        if "szimaszgi" in cleaned and not any(a in cleaned for a in SORTED_ANIMAL_TERMS):
            continue
        # -am3 suffix (copula, "it is N"): e.g. "ka-lu 1(gesz2) 3(u)-am3"
        if re.search(r"\d+\([^)]+\)-am3", cleaned):
            continue
        # Summary lines with unrecognized terms — downgrade to info
        if is_summary_line(line):
            issues.append({
                "type": "unrecognized_after_numeral",
                "severity": "info",
                "line": cleaned,
                "message": "Summary line with unrecognized term after numeral",
            })
            continue
        issues.append({
            "type": "unrecognized_after_numeral",
            "severity": "info" if heavily_damaged else "warning",
            "line": cleaned,
            "message": "Numeral found but no recognized animal term follows",
        })

    # Possible theophoric false positives in divine recipients
    person_name_strs = {p.name for p in extraction.persons}
    flagged_lines = set()
    for deity in extraction.divine_recipients:
        for line in lines:
            cleaned = strip_atf_damage(line)
            if deity not in cleaned:
                continue
            if any(deity in pname for pname in person_name_strs):
                continue
            idx = cleaned.find(deity)
            before = cleaned[:idx]
            after = cleaned[idx + len(deity):]
            # Deity is standalone (preceded/followed by space or line boundary): not theophoric
            is_standalone = (not before or before[-1] in (" ", "\t")) and \
                            (not after or after[0] in (" ", "\t") or after.startswith(("-sze3", "-ra", "-ke4")))
            if is_standalone:
                continue
            # Deity embedded in a compound (hyphen-linked): clearly theophoric personal name
            is_theophoric = (before and before[-1] == "-") or (after and after[0] == "-")
            line_key = (deity, cleaned)
            if line_key in flagged_lines:
                continue
            flagged_lines.add(line_key)
            issues.append({
                "type": "possible_theophoric",
                "severity": "info" if is_theophoric else "warning",
                "line": cleaned,
                "message": f"Deity {deity} may be part of a personal name",
            })

    # No transaction type
    if not extraction.transaction_type and lines:
        issues.append({
            "type": "no_transaction",
            "severity": "info",
            "line": "",
            "message": "No transaction type detected",
        })

    # Person-role markers present but no persons extracted
    if not extraction.persons and lines:
        has_source = any(re.search(r"ki\s+\S+-ta", strip_atf_damage(l)) for l in lines)
        has_szu = any("szu ba-ti" in strip_atf_damage(l) for l in lines)
        has_dab = any("i3-dab5" in strip_atf_damage(l) for l in lines)
        has_giri = any("giri3 " in strip_atf_damage(l) for l in lines)
        if has_source or has_szu or has_dab or has_giri:
            issues.append({
                "type": "missed_persons",
                "severity": "warning",
                "line": "",
                "message": "Person role markers found but no persons extracted",
            })

    # Edge total vs extracted total mismatch
    if extraction.edge_total > 0 and extraction.total_animals > 0:
        if extraction.edge_total != extraction.total_animals:
            # Skip if tablet is heavily damaged — mismatch is expected
            if extraction.damage.preservation < 0.6:
                pass
            else:
                summary_is_damaged = any(
                    ("szu-nigin" in strip_atf_damage(l) or "szunigin" in strip_atf_damage(l))
                    and has_damage(l)
                    for l in lines
                )
                if not summary_is_damaged:
                    diff = abs(extraction.edge_total - extraction.total_animals)
                    # Severity based on magnitude and preservation
                    if diff <= 2:
                        severity = "info"
                    elif extraction.damage.preservation < 0.8:
                        severity = "warning"
                    else:
                        severity = "error" if diff > 5 else "warning"
                    issues.append({
                        "type": "total_mismatch",
                        "severity": severity,
                        "line": str(extraction.edge_total),
                        "message": f"Edge total {extraction.edge_total} ≠ extracted total {extraction.total_animals} (diff={diff})",
                    })

    # Implicit count=1 (informational)
    for a in extraction.animals:
        if a.count == 1 and not NUMERAL_PATTERN.search(a.raw):
            issues.append({
                "type": "implicit_count",
                "severity": "info",
                "line": a.raw,
                "message": f"Implicit count=1 for {a.animal}",
            })

    return issues


# ---------------------------------------------------------------------------
# Record linkage / duplicate detection (Entity Resolution)
# ---------------------------------------------------------------------------

def _split_names(field: str) -> list[str]:
    """Split a CSV person-field ('Nasa; Abba-saga') into normalized tokens."""
    if not field:
        return []
    return [n.strip().lower() for n in field.split(";") if n.strip()]


def _name_overlap(a: list[str], b: list[str]) -> bool:
    """True if the two name lists share at least one token."""
    if not a or not b:
        return False
    return bool(set(a) & set(b))


def find_linked_records(extracted_path: Path, output_path: Path) -> int:
    """Identify double-entry tablets via composite-key record linkage.

    A linked pair (A, B) shares:
        – Temporal anchor:    same ruler, regnal year, month, day
        – Quantitative anchor: same resolved animal total (status != incomplete)
        – Network anchor:      source OR receiver name overlap (normalized)

    The same historical event is often booked twice — once at Puzriš-Dagan
    and once at the partner archive (Umma, Nippur, the disbursing office) —
    so these pairs validate the transaction across the network. They are
    flagged, not deduplicated.

    Returns the number of linked pairs written.
    """
    rows = []
    with open(extracted_path, "r", encoding="utf-8") as infile:
        for row in csv.DictReader(infile):
            # Skip rows we cannot anchor in time, count, or network
            ruler = row.get("ruler", "").strip()
            year = row.get("regnal_year", "").strip()
            month = row.get("month_number", "").strip()
            day = row.get("day", "").strip()
            total = row.get("total_animals_resolved", "").strip()
            status = row.get("total_animals_status", "complete")
            if not ruler or not year or not month or not day:
                continue
            if status == "incomplete" or total in ("", "NaN", "0"):
                continue
            try:
                total_int = int(total)
            except ValueError:
                continue
            rows.append({
                "id": row["tablet_id"],
                "designation": row.get("designation", ""),
                "ruler": ruler,
                "year": year,
                "month": month,
                "day": day,
                "total": total_int,
                "source": _split_names(row.get("source_normalized", "")),
                "receiver": _split_names(row.get("receiver_normalized", "")),
                "tx": row.get("transaction_type", ""),
            })

    # Bucket by (date, total). Within each bucket, compare names.
    buckets: dict[tuple, list[dict]] = {}
    for r in rows:
        key = (r["ruler"], r["year"], r["month"], r["day"], r["total"])
        buckets.setdefault(key, []).append(r)

    pairs = []
    for key, group in buckets.items():
        if len(group) < 2:
            continue
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                a, b = group[i], group[j]
                src_match = _name_overlap(a["source"], b["source"])
                rcv_match = _name_overlap(a["receiver"], b["receiver"])
                # Network anchor: at least one of source/receiver must match.
                # An exact same-day same-total coincidence with no name link
                # is statistically common in this corpus and not informative.
                if not (src_match or rcv_match):
                    continue
                anchors = ["date", "quantity"]
                if src_match: anchors.append("source")
                if rcv_match: anchors.append("receiver")
                pairs.append({
                    "tablet_a": a["id"],
                    "tablet_b": b["id"],
                    "designation_a": a["designation"],
                    "designation_b": b["designation"],
                    "ruler": a["ruler"],
                    "regnal_year": a["year"],
                    "month": a["month"],
                    "day": a["day"],
                    "total_animals": a["total"],
                    "tx_a": a["tx"],
                    "tx_b": b["tx"],
                    "source_match": "1" if src_match else "0",
                    "receiver_match": "1" if rcv_match else "0",
                    "anchors_matched": "|".join(anchors),
                    "match_strength": len(anchors),
                })

    pairs.sort(key=lambda p: (-p["match_strength"], p["tablet_a"]))

    fields = [
        "tablet_a", "tablet_b", "designation_a", "designation_b",
        "ruler", "regnal_year", "month", "day", "total_animals",
        "tx_a", "tx_b",
        "source_match", "receiver_match",
        "anchors_matched", "match_strength",
    ]
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for p in pairs:
            writer.writerow(p)

    print(f"Found {len(pairs)} linked record pairs → {output_path}")
    return len(pairs)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    base = Path(__file__).parent
    process_database(
        input_path=base / "drehem_database.csv",
        output_path=base / "drehem_extracted.csv",
    )
    find_linked_records(
        extracted_path=base / "drehem_extracted.csv",
        output_path=base / "drehem_linked_records.csv",
    )
