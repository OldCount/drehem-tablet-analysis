# Thesis Context: Puzriš-Dagan Livestock Administration

**Working Title:** Animal Transport and Administration in Puzriš-Dagan: A Quantitative Analysis of the Ur III Livestock Archive
**Era:** Ur III Period (ca. 2056-2026 BCE / Šulgi 42 to Ibbi-Suen 2)
**Location:** Puzriš-Dagan (modern Drehem, southern Iraq)

## The Core Problem
Puzriš-Dagan was the central livestock clearinghouse for the Neo-Sumerian empire. Tens of thousands of animals passed through annually for cultic offerings, the royal court, and diplomacy. 

While the administrative structure has been manually reconstructed by scholars (Sigrist 1992, Tsouparopoulou 2013, Liu 2019) into four main bureaus (Main, Disbursement, Herding, Dead Animals), **a sweeping quantitative analysis of distribution patterns across the entire ~15,000 tablet corpus is missing.** 

The highly formalized Sumerian administrative language makes these texts perfect for automated data extraction. This project aims to use algorithmic extraction to reveal macro-patterns that manual, selective reading misses.

## Primary Research Question
What patterns in livestock distribution become visible when the Puzriš-Dagan corpus is systematically processed via automated data extraction from transliterations, and how do these patterns relate to the existing historiography of Ur III administration?

## Sub-questions to Explore
- Are there seasonal patterns in intake/disbursement that map to the cultic calendar? (e.g. referencing Sallaberger 1993)
- Does the composition of the herds differ by bureau, official, or time period?
- How did the volume or nature of distribution change over the active lifespan of the archive?

## Scope & Methodology
- **Sources:** CDLI transliterations (not the physical tablets). Excludes non-animal transaction texts.
- **Data Extracted:** Species, count, transaction type (mu-kux/intake, i₃-dab₅/transfer, ba-zi/disbursement), date, destination, and involved officials/bureaus.
- **Approach:** Digital Humanities as a means, not an end. The extraction algorithms identify patterns, but the interpretation remains strictly philological and historical.
- **Validation:** A manually translated reference corpus will be used to benchmark and validate the algorithm's accuracy (especially regarding damage `[...]` handling, species recognition, etc). 

## Key Historiographical Benchmarks
The data extracted from this pipeline will be directly compared against established literature to confirm known facts or reveal new insights. Key references include:
- **Sigrist (1992):** Foundational prosopography
- **Tsouparopoulou (2013):** Reconstruction of the 4-bureau system and the "dead animal" office
- **Liu (2019, 2021):** Detailed practices under Amar-Suen / Harvard tablet editions 
- **Steinkeller (1995):** Sheep and goat terminology
- **Sallaberger (1993):** Cultic calendar
- **Widell (2020):** Seasonal breeding patterns
