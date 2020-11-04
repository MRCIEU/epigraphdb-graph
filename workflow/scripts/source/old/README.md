### SemMedDB

- Create delimited versions of SQL files
- Filter to only incluse specific predicates and semantic types
- Takes a few minutes

### MELODI-Presto

- takes around 30 minutes

```
scripts/processing/gwas-melodi.py
```

### GWAS NLP

- takes around 30 minutes

```
scripts/processing/trait-nlp.py
```

### VEP

- Currently running on SSD machine
- Takes around ?

Setup VEP container

```
docker run -t -i -v /data/vep_data:/opt/vep/.vep ensemblorg/ensembl-vep perl INSTALL.pl -a cf -s homo_sapiens -y GRCh37
```

Create single list of variants

```
zless Variant.csv.gz | cut -f1 -d ',' > variants-24-08-20.csv
```

Run VEP container:

```
time docker run -t -i -v /data/vep_data:/opt/vep/.vep ensemblorg/ensembl-vep ./vep --port 3337 --cache --fork 20 --assembly GRCh37 -i /opt/vep/.vep/variants-24-08-20.csv -o /opt/vep/.vep/variants-24-08-20.txt --per_gene --no_intergenic
```

### Opentargets

- takes a few minutes
- creates file ot.csv
- run on any machine

```
python scripts/processing/opentargets.py
```

### Metamap-lite

- takes ~1 second to process 10 (30,000 = 50 minutes)
- run on SSD machine

```
python scripts/processing/metamap create
find ./data/metamap/sep-traits/ -name "*.txt" | parallel -j 20 /data/software/metamap-lite/public_mm_lite/metamaplite.sh  --segment_lines {}
python scripts/processing/metamap process
```

### XQTL

```
python scripts/processing/xqtl.py
```

### EFO

Download EFO via sparql
- note, wasn't working on 01/09/20

```
python -m scripts.processing.gwas-efo
```

Create GWAS to EFO NLP scores via vectology
- todo (currenty done via notebook :( )

### SemRep ArXiv

https://ieugit-scmv-d0.epi.bris.ac.uk/be15516/semrep-arxiv