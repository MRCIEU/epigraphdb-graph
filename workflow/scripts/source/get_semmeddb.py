import os
import sys
import pandas as pd
import wget
from loguru import logger
from workflow.scripts.utils.general import copy_source_data

data_name = 'semmeddb'

#load semmemd tab separated predication to filter and create smaller version
PREDICATION_FILE = sys.argv[1]

# data are downloaded from here https://ii.nlm.nih.gov/SemRep_SemMedDB_SKR/SemMedDB/SemMedDB_download.shtml
# due to the license requirements, there doesn't seem to be a way to download the data easily
# need to download the PREDICATION and CITATION CSV files

def process():
    df = pd.read_csv(PREDICATION_FILE)
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
    logger.info(f'\n{df.head()}')
    logger.info(f"\n{df['subject_novelty'].value_counts()}")
    logger.info(f"\n{df['object_novelty'].value_counts()}")
    sub_nov = df[df.subject_novelty==0]['subject_name'].value_counts()
    logger.info(sub_nov)
    logger.info(df.shape)

    # filter to only keep rows with both subject and object novelty of 1
    df = df[(df['subject_novelty']==1) & (df['object_novelty']==1)]
    logger.info(df.shape)
    logger.info(f'\n{df.head()}')
    filename = f'/tmp/{PREDICATION_FILE}.filter.gz'
    df.to_csv(filename,index=False)
    copy_source_data(data_name=data_name,filename=filename)

if __name__ == "__main__":
    process()
    
