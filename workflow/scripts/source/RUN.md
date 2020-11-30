### Creating the raw/pre-processed source data

```
#opengwas
python -m workflow.scripts.source.get_opengwas

#opentargets
python -m workflow.scripts.source.get_opentargets

#reactome
python -m workflow.scripts.source.get_reactome

#Genetic Correlation
wget https://www.dropbox.com/sh/qvuil7op8bw68fm/AADgKY_MaVlwt6P7USmOk4oWa/geno_correlation.r2.gz

#biomart
python -m workflow.scripts.source.get_biomart

#CPIC
wget -O cpicPairs-`date +"%d-%m-%y"`.csv https://api.cpicpgx.org/data/cpicPairs.csv

#Druggable Genes
https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6321762/bin/NIHMS80906-supplement-Table_S1.xlsx

#EBI GWAS
python -m workflow.scripts.source.get_ebigwas

#EFO
wget -O efo-v3.24.0.json https://github.com/EBISPOT/efo/releases/download/v3.24.0/efo.json

#GWAS NLP
python -m workflow.scripts.source.get_gwas_nlp
```