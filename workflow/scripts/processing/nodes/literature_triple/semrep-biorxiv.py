import os
import sys
import pandas as pd
import numpy as np
from loguru import logger

#################### leave me heare please :) ########################

from workflow.scripts.utils.general import setup, get_source

from workflow.scripts.utils.writers import (
    create_constraints,
    create_import,
)

# setup
args, dataDir = setup()
meta_id = args.name

# args = the argparse arguments (name and data)
# dataDir = the path to the working directory for this node/rel

#######################################################################

SEM = get_source(meta_id,1)

def make_id(row,sub_type):
    id_val = row[sub_type+'_id']
    if pd.isna(row[sub_type+'_id']):
        id_val = row[sub_type+'_gene_id']
    return id_val

def make_name(row,sub_type):
    id_val = row[sub_type+'_name']
    if pd.isna(row[sub_type+'_name']):
        id_val = row[sub_type+'_gene_name']
    return id_val

def process():
    logger.info("loading semrep data... {}",SEM)
    sem_df = pd.read_csv(os.path.join(dataDir, SEM), sep=",", compression="gzip")

    #create new ids, if a gene standard id is empty
    logger.info('Dealing with IDs')
    sub_id = sem_df.apply(lambda row : make_id(row,'sub'), axis = 1)
    obj_id = sem_df.apply(lambda row : make_id(row,'obj'), axis = 1)
    sem_df['sub_id_all']=sub_id
    sem_df['obj_id_all']=obj_id

    #create new names, if a gene standard name is empty
    sub_name = sem_df.apply(lambda row : make_name(row,'sub'), axis = 1)
    obj_name = sem_df.apply(lambda row : make_name(row,'obj'), axis = 1)
    sem_df['sub_name_all']=sub_name
    sem_df['obj_name_all']=obj_name

    # need to split subject and object ids by ,
    logger.info(sem_df.shape)
    sem_df = (
        sem_df.assign(sub_id_all=sem_df.sub_id_all.str.split(","))
        .explode("sub_id_all")
        .reset_index(drop=True)
    )
    logger.info(sem_df.shape)
    sem_df = (
        sem_df.assign(obj_id_all=sem_df.obj_id_all.str.split(","))
        .explode("obj_id_all")
        .reset_index(drop=True)
    )
    logger.info(sem_df.shape)

    sem_id = sem_df['sub_id_all']+':'+sem_df['pred']+':'+sem_df['obj_id_all']
    sem_name = sem_df['sub_name_all']+' '+sem_df['pred']+' '+sem_df['obj_name_all']
    logger.debug(sem_id)
    sem_df['id']=sem_id
    sem_df['name']=sem_name
    logger.info("\n{}", sem_df)
    logger.info(sem_df.shape)

    # merge
    keep_cols=[
        'sub_id_all',
        'pred',
        'obj_id_all',
        'id',
        'name'
    ]
    sem_df = sem_df[keep_cols]
    sem_df.rename(columns={'pred':'predicate','sub_id_all':'subject_id','obj_id_all':'object_id'},inplace=True)
    sem_df.drop_duplicates(subset=['id'],inplace=True)
    logger.info("\n{}", sem_df)
    logger.info(sem_df.shape)

    #drop nas/rows with empty string 
    sem_df.replace('', np.nan, inplace=True)
    sem_df.dropna(inplace=True)
    logger.info(sem_df.shape)

    create_import(df=sem_df, meta_id=args.name)

if __name__ == "__main__":
    process()
