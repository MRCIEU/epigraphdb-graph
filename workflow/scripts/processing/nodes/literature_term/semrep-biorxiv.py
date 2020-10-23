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

SEM = get_source(meta_id,1)

def make_id(row,sub_type):
    id_val = row[sub_type+'_id']
    if pd.isna(row[sub_type+'_id']):
        id_val = row[sub_type+'_gene_id']
    return id_val

def process():
    logger.info("loading semrep data...{}",SEM)
    sem_df = pd.read_csv(os.path.join(dataDir, SEM), sep=",", compression="gzip")

    #need to deal with cases where there is no id and only a gene_id
    logger.info('Dealing with IDs')
    sub_id = sem_df.apply(lambda row : make_id(row,'sub'), axis = 1)
    obj_id = sem_df.apply(lambda row : make_id(row,'obj'), axis = 1)
    sem_df['sub_id_all']=sub_id
    sem_df['obj_id_all']=obj_id

    logger.info("\n{}", sem_df)
    logger.info(sem_df.shape)

    # merge
    keep_cols=[
        'sub_name'
        'sub_type'
        'sub_id_all',
        'obj_id_all',
        'obj_name'
        'obj_type'
    ]

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

    # create series of subject/object names
    term_names = pd.concat([sem_df["sub_name"], sem_df["obj_name"]])
    # create series of subject/object types
    term_types = pd.concat([sem_df["sub_type"], sem_df["obj_type"]])
    # create series of subject/object ids
    term_ids = pd.concat([sem_df["sub_id_all"], sem_df["obj_id_all"]])
    # create new df
    term_df = pd.concat([term_names, term_types, term_ids], axis=1)
    term_df.columns = ["name", "type", "id"]

    term_df.drop_duplicates(inplace=True)
    logger.info("\n{}", term_df)
    logger.info(term_df.shape)

    #some ids are badly formatted, e.g. ",123" and the split creates bad rows. Need to drop these
    term_df.dropna(inplace=True)

    # some ids have multiple types - need to create a list of types for each ID
    # make a map of id to types
    id_type_dic = term_df.groupby(["id"])["type"].apply(list).to_dict()
    # make lists unique
    for i in id_type_dic:
        l = ";".join(list(set(id_type_dic[i])))
        id_type_dic[i] = l
    # add type lists back as ; separated array
    term_df["type"] = term_df["id"].map(id_type_dic)
    term_df.drop_duplicates(inplace=True)
    logger.info("\n {}", term_df)

    create_import(df=term_df, meta_id=args.name)

    # create constraints
    constraintCommands = [
        "CREATE CONSTRAINT ON (s:LiteratureTerm) ASSERT s.id IS UNIQUE",
        "match (l:LiteratureTerm ) match (g:Gene) where toLower(g.name) = toLower(l.name) merge (l)-[:TERM_TO_GENE]->(g) return count(g);",
    ]
    create_constraints(constraintCommands, meta_id)

if __name__ == "__main__":
    process()
