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
    driver = neo4j_connect()
    session = driver.session()
    query = """
        match (d:Disease) unwind(d.efo) as mondo_efo_id return d.id as disease_id, mondo_efo_id;
    """
    query_data = session.run(query).data()
    df = pd.json_normalize(query_data)
    logger.info(df)
    return df


def process():
    data = os.path.join(dataDir, FILE)
    # not sure why double quotes weren't being handled properly, added engine param
    df = pd.read_csv(data, sep=",", engine="python")
    logger.info(df.shape)
    logger.info("\n {}", df.head())

    #get disease data 
    disease_df = get_disease_data()
    disease_df['mondo_efo_id'] = 'http://www.ebi.ac.uk/efo/EFO_'+disease_df['mondo_efo_id'].astype(str)
    logger.info(disease_df)

    keep_cols = [
        "molecule_name",
        "efo_id",
    ]
    df = df[keep_cols]

    mondo_match = pd.merge(df,disease_df,left_on='efo_id',right_on='disease_id')[['molecule_name','efo_id']]
    #logger.info(mondo_match)

    efo_match = pd.merge(df,disease_df,left_on='efo_id',right_on='mondo_efo_id')[['molecule_name','efo_id']]
    #logger.info(efo_match)

    cat_df = pd.concat([mondo_match,efo_match])
    logger.info(cat_df.shape)

    cat_df.drop_duplicates(inplace=True)
    logger.info(cat_df.shape)

    col_names = ["source", "target"]
    cat_df.columns = col_names
    cat_df["source"] = cat_df["source"].str.upper()

    create_import(df=cat_df, meta_id=meta_id)

if __name__ == "__main__":
    process()