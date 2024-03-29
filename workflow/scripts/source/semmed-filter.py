import os
import sys
import pandas as pd
from loguru import logger

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
    df = pd.read_csv(PREDICATION_FILE,sep='\t')
    col_names=[
        'predication_ID',
        'sentence_ID',
        'PMID',
        'predicate',
        'subject_id',
        'subject_name',
        'subject_type',
        'subject_novelty',
        'object_id',
        'object_name',
        'object_type',
        'object_novelty',
        'x',
        'y',
        'z',
        ]
    
    df.columns=col_names
    logger.info(df.shape)
    #filter on predicates
    df = df[~df.predicate.isin(predIgnore)]
    logger.info(df.shape)
    #filter on types
    df = df[df.subject_type.isin(typeFilterList)]
    df = df[df.object_type.isin(typeFilterList)]
    logger.info(df.shape)

    df.to_csv(PREDICATION_FILE+'.filter',compression='gzip',index=False)



if __name__ == "__main__":
    process()
    
