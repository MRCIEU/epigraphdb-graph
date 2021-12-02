import os
import sys
import pandas as pd
from loguru import logger
#################### leave me heare please :) ########################

from workflow.scripts.utils.general import setup, get_source, neo4j_connect

from workflow.scripts.utils.writers import (
    create_constraints,
    create_import
)

# setup
args, dataDir = setup()
meta_id = args.name

# args = the argparse arguments (name and data)
# dataDir = the path to the working directory for this node/rel

#######################################################################

FILE = get_source(meta_id,1)

def get_disease_data():
    # query the graph for all mondo_ids and efo_ids separately and then merge them on mondo_id col
    # we have to do two separate queries because efo column has to be unwind, and
    # that command retains only rows where the operation is performed, so we lose all mondo_ids that have nan in efo col

    driver = neo4j_connect()
    session = driver.session()

    query1 = """ match (d:Disease) return d.id as mondo_id """
    query_data1 = session.run(query1).data()
    mondo_only = pd.json_normalize(query_data1)

    query2 = """match (d:Disease) unwind(d.efo) as efo_id return d.id as mondo_id , efo_id"""
    query_data2 = session.run(query2).data()
    mondo_w_efo = pd.json_normalize(query_data2)
    mondo_w_efo['efo_id'] = 'http://www.ebi.ac.uk/efo/EFO_' + mondo_w_efo['efo_id'].astype(str)

    disease_df = pd.merge(mondo_only, mondo_w_efo, how='outer', on='mondo_id')
    disease_df = disease_df.drop_duplicates()
    return disease_df


def process():
    data = os.path.join(dataDir, FILE)
    # not sure why double quotes weren't being handled properly, added engine param
    df = pd.read_csv(data, sep=",", engine="python")
    df = df.rename(columns={"efo_id": "disease_id"})
    logger.info(df.shape)
    logger.info("\n {}", df.head())
    df = df[["molecule_name", "disease_id"]]

    # get disease data from the graph
    disease_df = get_disease_data()
    logger.info(disease_df)

    # join df (OT data) and disease_df (graph) on mondo_id
    mondo_match = pd.merge(df, disease_df, left_on='disease_id', right_on='mondo_id')[['molecule_name', 'disease_id']]
    mondo_match.drop_duplicates(inplace=True)
    # logger.info(mondo_match)

    # join on efo_id, but keep the corresponding mondo_id, as this is what used for mapping
    efo_match = pd.merge(df, disease_df, left_on='disease_id', right_on='efo_id')[['molecule_name', 'mondo_id']]
    efo_match = efo_match.rename(columns={"mondo_id": "disease_id"})
    efo_match.drop_duplicates(inplace=True)
    # logger.info(efo_match)

    cat_df = pd.concat([mondo_match, efo_match])
    logger.info(cat_df.shape)

    cat_df.drop_duplicates(inplace=True)
    logger.info(cat_df.shape)

    col_names = ["source", "target"]
    cat_df.columns = col_names
    cat_df["source"] = cat_df["source"].str.upper()

    create_import(df=cat_df, meta_id=args.name)

if __name__ == "__main__":
    process()
