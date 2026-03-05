# Drehem Tablet Analysis

This project is a specialized data extraction and visualization pipeline for Ur III administrative clay tablets from Puzrish-Dagan (modern Drehem). It processes raw transliteration data (originally sourced from the Cuneiform Digital Library Initiative) to extract semantic information and provides an interactive visualizer for analysis and demonstration.

## Features

- **Transliteration Parsing (`drehem_extract.py`)**: Custom ATF (ASCII Text Format) parser that identifies animals, persons, administrative roles, geographical destinations, and date formulas.
- **Semantic Annotation (`tablet_visualizer.py`)**: Token-level classification of Sumerian terms (e.g., animals, qualifiers, numerals, structural markers, person names, deities).
- **Interactive Visualization (`tablet_vis.html`)**: A local web interface providing visual syntax highlighting, cross-referencing, translation (English/Dutch), and extracted metadata panels.
- **ORACC Name Integration**: Built-in support for matching and normalizing names against the ORACC name dictionary.

## Project Structure

- `drehem_extract.py`: Core extraction engine for extracting entities and relationships.
- `tablet_visualizer.py`: Annotation pipeline that structures data for the frontend and serves it locally.
- `tablet_vis.html`: Interactive frontend for tablet visualization.
- `name_search.html` & `build_name_dictionary.py`: Interface and tooling for the ORACC name normalization dictionary.
- `drehem.sh`: Command-line utility script for running extraction pipelines.
- `Open Dashboard.command`: macOS shortcut to launch the visualizer.
- `logs/`: Session worklogs tracking development progress and technical decisions.
- `_archive/` & `backups/`: (Local only) Superseded scripts and full data backups.

## Usage

To start the interactive tablet visualizer, launch the local server:

```bash
python3 tablet_visualizer.py --port 8585
```

Navigate to `http://localhost:8585` in your browser.

Alternatively, on macOS, you can simply run the `Open Dashboard.command` script.

## Background & Data Sources

This tool is being developed as part of a bachelor's thesis project focusing on data engineering and the quantitative analysis of Ur III animal administration. The underlying transliteration text data originates from the public catalogue of the [Cuneiform Digital Library Initiative (CDLI)](https://cdli.ucla.edu/).
