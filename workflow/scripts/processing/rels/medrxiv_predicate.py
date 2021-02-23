import os
import sys
import pandas as pd
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

FILE = get_source(meta_id,1)

def make_id(row,sub_type):
    id_val = row[sub_type+'_id']
    if pd.isna(row[sub_type+'_id']):
        id_val = row[sub_type+'_gene_id']
    return id_val

def process():
    logger.info("loading semrep data...{}",FILE)
    sem_df = pd.read_csv(os.path.join(dataDir, FILE), sep=",", compression="gzip")
    logger.info(sem_df)
    #create new ids 
    logger.info('Dealing with IDs')
    sub_id = sem_df.apply(lambda row : make_id(row,'sub'), axis = 1)
    obj_id = sem_df.apply(lambda row : make_id(row,'obj'), axis = 1)
    sem_df['sub_id_all']=sub_id
    sem_df['obj_id_all']=obj_id

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

    keep_cols = ["sub_id_all", "pred", "obj_id_all"]
    sem_df = sem_df[keep_cols]
    sem_df = pd.DataFrame({"count": sem_df.groupby(keep_cols).size()}).reset_index()
    logger.info(sem_df.shape)
    sem_df.drop_duplicates(inplace=True)
    logger.info(sem_df.shape)
    sem_df.columns = ["source", "predicate", "target", "count"]
    logger.info("\n {}", sem_df)
    create_import(df=sem_df, meta_id=meta_id)


if __name__ == "__main__":
    process()
