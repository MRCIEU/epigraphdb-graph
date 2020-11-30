# Creating the raw/pre-processed source data

### OpenGWAS

- Fetches metadata for GWAS nodes via OpenGWAS API
- Fetches tophits for each GWAS via OpenGWAS API
- http://gwasapi.mrcieu.ac.uk/

```
python -m workflow.scripts.source.get_opengwas
```

### OpenTargets

Uses OpenTargets API

- https://api.opentargets.io/v3

```
python -m workflow.scripts.source.get_opentargets
```

### Reactome

Downloads files from Reactome and filters for human
- https://reactome.org/download-data

```
python -m workflow.scripts.source.get_reactome
```

### Genetic Correlation

Ben Neale genetic correlation data

```
wget https://www.dropbox.com/sh/qvuil7op8bw68fm/AADgKY_MaVlwt6P7USmOk4oWa/geno_correlation.r2.gz
```

### Biomart

Uses the biomart python package to download gene and protein data

```
python -m workflow.scripts.source.get_biomart
```

### CPIC

Download from websit
- https://api.cpicpgx.org/data/

```
wget -O cpicPairs-`date +"%d-%m-%y"`.csv https://api.cpicpgx.org/data/cpicPairs.csv
```

### Druggable Genes

Download from supplementarty material
- https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6321762/
- https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6321762/bin/NIHMS80906-supplement-Table_S1.xlsx

### EBI GWAS

Get the EBI GWAS Catalog EFO annotations and filter by those GWAS in OpenGWAS
- https://www.ebi.ac.uk/gwas/api/search/downloads/studies_alternative

```
python -m workflow.scripts.source.get_ebigwas
```

### EFO

Todo:
- Easy enough to get EFO data but harder to find parent/child relationships

```
wget -O efo-v3.24.0.json https://github.com/EBISPOT/efo/releases/download/v3.24.0/efo.json
```

### GWAS NLP

- Get all GWAS trait names from OpenGWAS
- Filter using Vectology filtering rules
- Create embeddings for each using Vectology
- Create all-against-all cosine distances

```
python -m workflow.scripts.source.get_gwas_nlp
```

### GTeX

Direct download from website

```
wget https://storage.googleapis.com/gtex_analysis_v8/rna_seq_data/GTEx_Analysis_2017-06-05_v8_RNASeQCv1.1.9_gene_median_tpm.gct.gz
```

### GWAS MELODI

Get literature enrichment annotations for each GWAS trait using MELODI-Presto
- https://melodi-presto.mrcieu.ac.uk/api/

```
python -m workflow.scripts.source.get_gwas_melodi
```

### MetaMap

Used to create link between GWAS trait names and UMLS Literature Terms

- Need to be run on machine with metamap installed

```
python -m workflow.scripts.source.get_metamap create
find ./data/metamap/sep-traits/ -name "*.txt" | parallel -j 20 /data/software/metamap-lite/public_mm_lite/metamaplite.sh  --segment_lines {}
python -m workflow.scripts.source.get_metamap process
```

### Mondo

Download directly from Mondo 

```
wget http://purl.obolibrary.org/obo/mondo.json
```

### MR-EvE

https://www.biorxiv.org/content/10.1101/173682v2

### SemMedDB

Requires a UTS license - https://uts.nlm.nih.gov/uts/signup-login

Download PREDICATION and CITATIONS SQL files from here - https://ii.nlm.nih.gov/SemRep_SemMedDB_SKR/SemMedDB/SemMedDB_download.shtml
- semmedVERXXXXX_PREDICATION.sql.gz
- semmedVERXXXXX_CITATIONS.sql.gz

Convert to TSV

```
python worklow/scripts/source/mysql_to_csv.py <(gunzip -c semmedXXXXX_PREDICATION.sql.gz) | gzip > semmedXXXXX_PREDICATION.tsv.gz
python worklow/scripts/source/mysql_to_csv.py <(gunzip -c semmedXXXXX_CITATIONS.sql.gz) | gzip > semmedXXXXX_CITATIONS.tsv.gz
```

Create filtered version:

```
python workflow/scripts/source/semmed_filter.py semmedXXXXX_PREDICATION.tsv.gz
```

### SemRep Arxiv

Todo

### StringDB

Download directly from StringDB
- https://stringdb-static.org/download/

```
wget https://stringdb-static.org/download/protein.links.v11.0/9606.protein.links.v11.0.txt.gz
wget https://stringdb-static.org/download/mapping_files/uniprot/human.uniprot_2_string.2018.tsv.gz
```

### PRS Atlas

To do

### UKB Phenotype Correlation

To do

### VEP

To do

### xQTL

To do