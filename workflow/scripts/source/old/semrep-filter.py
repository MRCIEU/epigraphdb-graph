import os
import sys
import pandas as pd
from loguru import logger

#python scripts/processing/semrep-filter.py medrxiv_semrep.csv.gz
#python scripts/processing/semrep-filter.py biorxiv_semrep.csv.gz

#load semmemd tab separated predication to filter and create smaller version
PREDICATION_FILE = sys.argv[1]

def process():
    #applying same filters as MELODI Presto
    predIgnore = [
        "PART_OF",
        "ISA",
        "LOCATION_OF",
        "PROCESS_OF",
        "ADMINISTERED_TO",
        "METHOD_OF",
        "USES",
        "compared_with",
    ]
    # typeFilterList = ["aapp","dsyn","enzy","gngm","chem","clnd","horm","hops","inch","orch"]
    typeFilterList = [
        "aapp",
        "clnd",
        "clna",
        "dsyn",
        "enzy",
        "gngm",
        "chem",
        "clnd",
        "horm",
        "hops",
        "inch",
        "orch",
        "phsu",
    ]

    #load predicate data
    logger.info('loading data...')
    df = pd.read_csv(PREDICATION_FILE,sep=',')
    col_names=[
        "sub_id",
        "sub_name",
        "sub_type",
        "sub_gene_id",
        "sub_gene_name",
        "pred",
        "obj_id",
        "obj_name",
        "obj_type",
        "obj_gene_id",
        "obj_gene_name",
        "doi",
        "section",
    ]
    
    df.columns=col_names
    logger.info(df.shape)
    #filter on predicates
    df = df[~df.pred.isin(predIgnore)]
    logger.info(df.shape)
    #filter on types
    df = df[df.sub_type.isin(typeFilterList)]
    df = df[df.obj_type.isin(typeFilterList)]
    logger.info(df.shape)

    df.to_csv(PREDICATION_FILE+'.filter',compression='gzip',index=False)



if __name__ == "__main__":
    process()
    
