#!/bin/bash

download_data () {
  if [ ! -f "$1"  ]; then
    wget $2 \
         -O $1
  else
    echo "$1 exists"
  fi

}

mkdir -p data

# main evidence dataset
download_data \
  "data/genotype-phenotype-assertions-all.csv.gz" \
  http://ftp.ebi.ac.uk/pub/databases/impc/all-data-releases/latest/results/genotype-phenotype-assertions-ALL.csv.gz

# Mouse/Human Orthology with Phenotype Annotations
# used as mouse gene to human gene mapping
download_data \
  "data/HMD_HumanPhenotype.rpt" \
  http://www.informatics.jax.org/downloads/reports/HMD_HumanPhenotype.rpt

# Mouse Genotypes with both Phenotype and Disease Annotations
# used as mouse phenotype to human disease mapping
download_data \
  "data/MGI_Geno_DiseaseDO.rpt" \
  http://www.informatics.jax.org/downloads/reports/MGI_Geno_DiseaseDO.rpt
download_data \
  "data/MGI_Geno_NotDiseaseDO.rpt" \
  http://www.informatics.jax.org/downloads/reports/MGI_Geno_NotDiseaseDO.rpt

# mammalian phenotype ontology
download_data \
  "data/mp.json" \
  http://www.informatics.jax.org/downloads/reports/mp.json

# mondo disease ontology
download_data \
  "data/mondo.json" \
  http://purl.obolibrary.org/obo/mondo.json
download_data \
  "data/hp.json" \
  http://purl.obolibrary.org/obo/hp.json
