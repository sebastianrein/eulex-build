# EULEX-BUILD

**A Python Pipeline for Building Research-Ready Datasets from EU Legislation**

EULEX-BUILD is a specialized tool designed for researchers studying European Union legislation. It automates the process
of collecting, parsing, and structuring legal documents from EUR-Lex into analysis-ready datasets suitable for legal
research, network analysis, natural language processing, and policy studies.

### Key Features

- **🎯 Flexible Document Selection**: Choose documents by specific CELEX IDs, legislative procedure numbers, or query by
  date range, document type, and
  EuroVoc keywords
- **📄 Comprehensive Text Extraction**: Extract structured text from recitals, articles, and annexes
- **🔗 Relationship Mapping**: Automatically capture citations, amendments, repeals, and other inter-document
  relationships
- **📊 Multiple Export Formats**: Export to CSV, Parquet, and SQLite for seamless integration with your analysis workflow
- **🔍 EuroVoc Integration**: Filter documents by policy domain using EU's multilingual thesaurus
- **📝 Detailed Documentation**: Logging files explain your dataset structure and provenance

## Installation

Run the following command in your terminal:

```bash
pip install git+https://github.com/sebastianrein/eulex-build.git
```

If you want to amend the source code, clone the repository and install in editable mode:

```bash
git clone https://github.com/sebastianrein/eulex-build.git
cd eulex-build
pip install -e .
```

## Quick Start

### 1. Create a Configuration File

Create a YAML configuration file that defines your dataset. Here's an extended example:

```yaml
metadata:
  project_name: "EU Environmental Law Dataset"
  author: "Your Name"
  description: "EU environmental regulations 2015-2024"
  version: 1.0

data:
  mode: "descriptive"  # or "fixed" for specific CELEX IDs or procedure numbers
  document_types:
    - "regulation"
    - "directive"
  start_date: 2015-01-01
  end_date: 2024-12-31
  filter_keywords:
    - "environment"
    - "climate change"
  include_corrigenda: false
  include_consolidated_texts: false
  include_national_transpositions: false

processing:
  enable_parallel_processing: true
  max_threads: 4
  text_extraction:
    include_recitals: true
    include_articles: true
    include_annexes: false
  relations_extraction:
    include_relations: true
    include_original_act_relations_for_consolidated_texts: false

output:
  include_raw_full_text: false
  formats:
    - "csv"
    - "parquet"
  output_directory: "./output"
```

### 2. Run the Pipeline

#### Option A: Using Python code

```python
from eulexbuild import EULEXBuildPipeline

# Initialize and run the pipeline
pipeline = EULEXBuildPipeline("configuration.yaml")
pipeline.run()
```

#### Option B: Using the command-line interface (CLI)

```bash
eulexbuild run configuration.yaml
```

You can also specify a custom database name:

```bash
eulexbuild run configuration.yaml --db-name my_dataset.db
```

### 3. Access Your Data

The pipeline creates three interconnected datasets:

**`works`** - Document metadata (one row per document)

- CELEX ID, title, adoption date, document type

**`text_units`** - Extracted text segments (multiple rows per document)

- Text from recitals, articles, and annexes with structural identifiers

**`relations`** - Inter-document relationships

- Citations, amendments, repeals, and other legal connections

## Configuration Guide

### Configuration Options Reference

The configuration file is divided into four main sections: `metadata`, `data`, `processing`, and `output`. Below is a
complete reference of all available options.

#### Metadata Section

| Option         | Description                                      | Required | Default                                         |
|----------------|--------------------------------------------------|----------|-------------------------------------------------|
| `project_name` | Name of your dataset project                     | No       | `"EULEX-BUILD Dataset"`                         |
| `author`       | Author name or institution                       | No       | `""` (empty string)                             |
| `description`  | Brief description of dataset scope and purpose   | No       | `"A new dataset constructed with EULEX-BUILD."` |
| `date_created` | Date the dataset was created (YYYY-MM-DD format) | No       | Today's date                                    |
| `version`      | Version number of the dataset                    | No       | `"1.0"`                                         |

#### Data Section

The `data` section requires you to specify a `mode` (either `"descriptive"` or `"fixed"`), which determines which other
options are available.

##### Common option:

| Option | Description                                           | Required | Default |
|--------|-------------------------------------------------------|----------|---------|
| `mode` | Document selection mode: `"descriptive"` or `"fixed"` | Yes      | None    |

##### Fixed Mode Options (when `mode: "fixed"`):

| Option              | Description                                                    | Required                | Default |
|---------------------|----------------------------------------------------------------|-------------------------|---------|
| `celex_ids`         | List of specific CELEX IDs to include in dataset               | At least one of the two | None    |
| `procedure_numbers` | List of legislative procedure numbers (e.g., "2023/0202(COD)") | At least one of the two | None    |

**Note:** You must provide at least one entry in either `celex_ids` or `procedure_numbers` (or both). Procedure numbers
are automatically resolved to their corresponding CELEX IDs by querying EUR-Lex.

##### Descriptive Mode Options (when `mode: "descriptive"`):

| Option                            | Description                                                                                     | Required | Default                                     |
|-----------------------------------|-------------------------------------------------------------------------------------------------|----------|---------------------------------------------|
| `document_types`                  | Types of legal acts to include: `"regulation"`, `"directive"`, `"decision"` and/or `"proposal"` | No       | `"regulation"`, `"directive"`, `"decision"` |
| `start_date`                      | Earliest adoption date for documents (YYYY-MM-DD, must be in the past)                          | Yes      | None                                        |
| `end_date`                        | Latest adoption date for documents (YYYY-MM-DD, must be in the past and after start_date)       | Yes      | None                                        |
| `filter_keywords`                 | List of keywords to filter documents via EuroVoc (empty list = no filtering)                    | No       | Empty list                                  |
| `include_corrigenda`              | Include technical corrections (corrigenda) documents                                            | No       | `false`                                     |
| `include_consolidated_texts`      | Include consolidated versions with amendments incorporated                                      | No       | `false`                                     |
| `include_national_transpositions` | Include national implementing measures                                                          | No       | `false`                                     |

#### Processing Section

| Option                       | Description                                                                        | Required | Default       |
|------------------------------|------------------------------------------------------------------------------------|----------|---------------|
| `enable_parallel_processing` | Use multiple CPU cores for faster processing                                       | No       | `true`        |
| `max_threads`                | Maximum number of parallel workers (capped at CPU count)                           | No       | CPU count - 1 |
| `automated_mode`             | Skip interactive EuroVoc review (for CI/CD pipelines); all fetched labels are used | No       | `false`       |

##### Text Extraction Sub-options (under `processing.text_extraction`):

| Option             | Description                                   | Required | Default |
|--------------------|-----------------------------------------------|----------|---------|
| `include_recitals` | Extract preamble/recital text from documents  | No       | `true`  |
| `include_articles` | Extract operative article text from documents | No       | `true`  |
| `include_annexes`  | Extract annex/appendix text from documents    | No       | `true`  |

##### Relations Extraction Sub-options (under `processing.relations_extraction`):

| Option                                                  | Description                                                                                                                           | Required | Default |
|---------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------|----------|---------|
| `include_relations`                                     | Extract inter-document relationships (citations, amendments, etc.)                                                                    | No       | `true`  |
| `include_original_act_relations_for_consolidated_texts` | For consolidated texts, also include relations from the original act (e.g., relations of the original GDPR for consolidated versions) | No       | `false` |

#### Output Section

| Option                  | Description                                                            | Required | Default      |
|-------------------------|------------------------------------------------------------------------|----------|--------------|
| `include_raw_full_text` | Include the raw HTML of full document text in the `works` table export | No       | `false`      |
| `formats`               | Export formats: `"csv"` and/or `"parquet"`                             | No       | Both formats |
| `output_directory`      | Directory path where output files will be saved                        | No       | `"./output"` |

**Note:** `include_raw_full_text` only affects the `works` table export in the requested formats. The raw HTML is still
included in the database

#### Notes:

- All date fields must be in `YYYY-MM-DD` format
- CELEX IDs are automatically normalized (spaces and hyphens removed, converted to uppercase)
- `max_threads` is automatically capped at your system's CPU count
- If you specify more threads than available CPUs, it will be reduced to match CPU count

### Data Selection Modes

EULEX-BUILD supports two modes for selecting documents:

#### Fixed Mode

Specify exact documents for your dataset using CELEX IDs and/or procedure numbers. 
At least one entry is required (either CELEX IDs or procedure numbers).
The pipeline queries EUR-Lex to find documents associated with each procedure number. 
If an adopted legal act exists (e.g., a regulation or directive), it uses that. 
If only a proposal exists, it uses the proposal document.

```yaml
data:
  mode: "fixed"
  celex_ids:
    - "32016R0679"
  procedure_numbers:
    - "2023/0202/COD"
    - "2023/0323(COD)"
```

**Required fields:** `mode`, and at least one of `celex_ids` or `procedure_numbers`  

#### Descriptive Mode

Automatically discover documents matching your criteria. Start and end dates are required.

```yaml
data:
  mode: "descriptive"
  document_types:
    - "regulation"    # EU Regulations
    - "directive"     # EU Directives  
    - "decision"      # EU Decisions
  start_date: 2010-01-01
  end_date: 2025-12-31
  filter_keywords:
    - "digital"
    - "artificial intelligence"
    - "data protection"
  include_corrigenda: false            # Technical corrections
  include_consolidated_texts: false    # Texts with amendments incorporated
  include_national_transpositions: false  # National implementing measures
```

**Required fields:** `mode`, `start_date`, `end_date`  
**Optional fields:** `document_types`, `filter_keywords`, `include_corrigenda`, `include_consolidated_texts`,
`include_national_transpositions`


When using `filter_keywords`, the pipeline will:

1. Query EuroVoc (EU's multilingual thesaurus) to find related concepts
2. Save matching EuroVoc labels to `eurovoc_labels.yaml` for review
3. Pause execution so you can refine the keyword mapping (unless `automated_mode` is enabled)
4. Use reviewed keywords to query EUR-Lex for matching documents

##### Automated Mode:

By default, when using `filter_keywords` in descriptive mode, the pipeline pauses for interactive review of EuroVoc
labels.
To skip, set `automated_mode: true`.
The labels are still saved to `eurovoc_labels.yaml` for record-keeping and audit purposes.

```yaml
processing:
  automated_mode: true  # Skip interactive EuroVoc review
```

### Processing Options

```yaml
processing:
  enable_parallel_processing: true  # Use multiple CPU cores
  max_threads: 4                    # Number of parallel workers
  automated_mode: false             # Set to true for CI/CD pipelines
  text_extraction:
    include_recitals: true          # Extract preamble/recitals
    include_articles: true          # Extract operative articles
    include_annexes: false          # Extract annexes/appendices
```

### Output Configuration

```yaml
output:
  formats:
    - "csv"
    - "parquet"   # Compressed, fast for large datasets
  output_directory: "./output"  # Where to save results
```

### Metadata

Document your dataset with descriptive metadata:

```yaml
metadata:
  project_name: "Your Dataset Name"
  author: "Your Name or Institution"
  description: "Brief description of the dataset scope and purpose"
  date_created: 2025-01-26
  version: 1.0
```

This metadata is included in the auto-generated dataset README file.

## Output Structure

The pipeline generates a complete, documented dataset in your output directory:

```
output/
├── README.md                 # Auto-generated dataset documentation
├── pipeline.log              # Detailed processing log
├── eulex_build.db            # SQLite database
├── works.csv / .parquet      # Document metadata
├── text_units.csv / .parquet # Extracted text segments
├── relations.csv / .parquet  # Document relationships
└── eurovoc_labels.yaml       # EuroVoc keyword mapping (if descriptive mode)
```

### Data Schema

#### Works Table

| Column           | Type   | Description                   | Example                               |
|------------------|--------|-------------------------------|---------------------------------------|
| `celex_id`       | string | Unique document identifier    | `32016R0679`                          |
| `title`          | string | Official document title       | "Regulation (EU) 2016/679..."         |
| `date_adopted`   | date   | Official adoption date        | `2016-04-27`                          |
| `document_type`  | string | Type of legal act             | `regulation`, `directive`, `decision` |
| `language`       | string | Language of extracted text    | `eng`                                 |
| `full_text_html` | string | Complete HTML of the document | `<html>...</html>` (nullable)         |

**Note:** The `full_text_html` column contains the complete HTML representation of each document as retrieved from
EUR-Lex. This is useful for custom text extraction, preserving document formatting, or performing advanced analysis that
requires the original document structure. The field may be `null` if the HTML could not be retrieved.

**Note:** The `date_adopted` field is automatically normalized to `YYYY-MM-DD` format. For consolidated CELEX IDs, the
date of the original act is used.

#### Text Units Table

| Column     | Type    | Description           | Example                                |
|------------|---------|-----------------------|----------------------------------------|
| `id`       | integer | Unique identifier     | `3`                                    |
| `celex_id` | string  | Parent document       | `32016R0679`                           |
| `type`     | string  | Type of content       | `recital`, `article`, `annex`          |
| `number`   | string  | Position in document  | `1`, `2(a)`, `Annex I`                 |
| `title`    | string  | Title of the section  | `Subject matter` (nullable)            |
| `text`     | string  | The actual legal text | "The protection of natural persons..." |

#### Relations Table

| Column          | Type    | Description          | Example                                  |
|-----------------|---------|----------------------|------------------------------------------|
| `id`            | integer | Unique identifier    | `3`                                      |
| `celex_source`  | string  | Referring document   | `32016R0679`                             |
| `celex_target`  | string  | Referenced document  | `31995L0046`                             |
| `relation_type` | string  | Type of relationship | `amends`, `repeals`, `based_on`, `cites` |


## Data Source

All data is retrieved from **EUR-Lex**, the official online database of EU law maintained by the Publications Office of
the European Union via the SPARQL endpoint and the REST API.

## Citation

If you use EULEX-BUILD in your research, please cite:

```bibtex
@software{eulex_build,
  author = {Sebastian Rein},
  title = {EULEX-BUILD: A Python Pipeline for EU Legislation Datasets},
  year = {2025},
  url = {https://github.com/sebastianrein/eulex-build}
}
```

## License

EULEX-BUILD is licensed under the MIT License. See [LICENSE](LICENSE) for details.

