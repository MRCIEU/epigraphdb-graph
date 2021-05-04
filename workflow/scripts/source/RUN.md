# Creating the raw/pre-processed source data

Much of this is covered in detail in the [EpiGraphDB supplementary material](https://oup.silverchair-cdn.com/oup/backfile/Content_public/Journal/bioinformatics/PAP/10.1093_bioinformatics_btaa961/2/btaa961_supplementary_data.docx?Expires=1609325340&Signature=HT4ZnYuWeYU46U~5qynSEc1Z0eAjPZc0e7z3VStcWtDN3C7U~oM9NB84EFhnqF8DW8IY-Czfwd4jbw6ErmhbAJ-wQOJyhJCA6cH6BoxPdlM2qoHFRtFj07rC3uMv2XovxzZiDP4yHsW23U7JqJYfnmpXqCkVL7YNAPVTGDfRcS4YTvA-~3C7gC-zBjoRXYla~RdzYkb~s6iWvdbIHfIBycMk7MCazMxTWE5VK6p-z2DxvtpLIwF5zKf6NdHI8R0sCB9oPL154h~D14~9l50BpvWCimelZyH3jEGzJVou1jppoPzxQbBB~6UG89ZAbxViAR2LN5nDmMMR4bcYpk~T9A__&Key-Pair-Id=APKAIE5G5CRDK6RD3PGA) 

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

### ClinVar

Download data from clinvar and process

```
python -m workflow.scripts.source.get_clinvar
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

Data downloaded from here
- https://datadryad.org/stash/dataset/doi:10.5061/dryad.h18c66b

### UKB Phenotype Correlation

We used PHESANT (Millard et al., 2018) to create a modified version of variables from the UK Biobank https://www.ukbiobank.ac.uk/. Using a basic spearman rank correlation, we produced pairwise correlation coefficients for all UK Biobank phenotypes for which we also have GWAS results from OpenGWAS (https://gwas.mrcieu.ac.uk/datasets/). These pairwise correlation coefficients create observational correlation relationships between the Gwas nodes.

- first impute the data to deal with missingness
- then correlate using spearman rank

```
Rscript correlate_ukb_obs_separate_files.R
```

### VEP

To do

### xQTL

Zheng J. et al. (2019) Systematic Mendelian Randomization and Colocalization Analyses of the Plasma Proteome and Blood Transcriptome to Prioritize Drug Targets for Complex Disease.

```
python workflow/scripts/source/xqtl.py

```
