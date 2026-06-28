# Evidence Chain in Built-Environment Analytics

Code and data-processing materials accompanying the manuscript:

**Evidence Chain in Built-Environment Analytics: A Corpus-Based Review of NSFC Project Records**

The study builds a reproducible evidence map of built-environment analytics from NSFC final-project records. The analytical corpus contains 9,222 deduplicated project records with valid award years from 2010 to 2023. The workflow uses a 37-term controlled vocabulary, a five-dimensional coding framework, and rule-coded project-record signals to summarize annual growth, evidence-base composition, recipient-organization geography, temporal keyword patterns, data-method-performance pathways, technical change, scale heterogeneity, topic clusters, thematic word clouds, and future research priorities.

## Repository Contents

- `01_data_loading_and_quality_audit.ipynb`  
  Audits the final analytical table for record counts, completeness, identifiers, duplicate rows, date parsing, and award-year quality.
- `02_screening_workflow_and_sample_construction.ipynb`  
  Documents retrieval, deduplication, screening-flow accounting, and sample construction.
- `03_annual_trend_analysis.ipynb`  
  Computes the 2010-2023 annual award-year distribution and exports the annual-growth figure/table.
- `04_coding_system_and_conceptual_framework.ipynb`  
  Builds the five-dimensional coding framework and conceptual framework figure.
- `05_review_overview_dashboard.ipynb`  
  Produces overview metrics for method families, built-environment objects, environmental-performance categories, scale cues, and maturity proxies.
- `06_spatial_distribution_analysis.ipynb`  
  Harmonizes recipient-organization geography and generates the institutional spatial distribution analysis.
- `07_topic_yearly_evolution_heatmap.ipynb`  
  Counts annual keyword signals and exports the temporal topic heatmap.
- `08_data_method_performance_pathways.ipynb`  
  Builds the data-source, method-family, and environmental-performance Sankey pathway analysis.
- `09_technology_change_and_scale_heterogeneity.ipynb`  
  Generates annual method-change and scale-heterogeneity summaries.
- `10_topic_clustering_and_keyword_network.ipynb`  
  Constructs the 37-keyword co-occurrence network and six empirical topic clusters.
- `11_topic_word_cloud_analysis.ipynb`  
  Produces topic-specific word-cloud terms and figures using controlled English labels.
- `00_NSFC_CN_advanced_search/`  
  Contains the collected NSFC advanced-search request-output CSV files used for the retrieval audit.

## Data Scope

This repository preserves the code-facing upload package. The advanced-search output CSV files are included under:

```text
00_NSFC_CN_advanced_search/output_requests_smart_grid/
```

Several analysis notebooks expect the manuscript analysis data in a project-level `data/` directory, especially:

```text
data/NSFC正式增量采集_2014-2026_去重筛选最终结果.csv
data/NSFC正式增量采集_37个关键词.csv
data/recipient_org_english_name_map.csv
data/recipient_org_geo_mapping.csv
data/china_province_basemap.geojson
```

The Chinese names in these data paths, NSFC query terms, project metadata, institution names, and matching dictionaries are intentionally retained because they are part of the source data and reproducible classification logic.

## Environment

The notebooks were developed for Python 3 and use common scientific Python packages, including:

```text
pandas
numpy
matplotlib
seaborn
geopandas
shapely
pyproj
cartopy
networkx
Pillow
requests
cryptography
pycryptodome
IPython
```

Some spatial visualization cells use map tiles and geospatial dependencies. If a tile service or local font is unavailable, the corresponding figure-export cells may need environment-specific configuration.

## Suggested Workflow

1. Clone the repository.
2. Create a Python environment with the packages listed above.
3. Place the required analytical data files in a `data/` directory at the project root.
4. Run the notebooks in numeric order if regeneration is needed.
5. Inspect generated files under `output/figures`, `output/tables`, and `output/logs`.

The README was prepared from the manuscript and static code inspection. The notebooks were not rerun during repository preparation.

## Citation

If you use this code, please cite the associated manuscript:

> Evidence Chain in Built-Environment Analytics: A Corpus-Based Review of NSFC Project Records.

## License

No license file is included in this upload package. Reuse permissions should be confirmed with the manuscript authors before redistribution.
