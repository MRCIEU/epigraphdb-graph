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

def process():
    data = os.path.join(dataDir, FILE)
    # not sure why double quotes weren't being handled properly, added engine param
    df = pd.read_csv(data, sep=",", engine="python")
    logger.info(df.shape)
    logger.info("\n {}", df.head())
    keep_cols = [
        "molecule_name",
        "efo_id",
    ]
    df = df[keep_cols]
    df["efo_id"] = df["efo_id"].str.split("_", expand=True)[1]
    logger.info(df.shape)
    logger.info("\n {}", df.head())
    col_names = ["source", "target"]
    df.columns = col_names
    df["source"] = df["source"].str.upper()
    df.drop_duplicates(inplace=True)
    logger.info(df.shape)
    logger.info("\n {}", df.head())
    create_import(df=df, meta_id=meta_id, import_type="load")

    # because this is mapping to a parameter that is not a node index, needs to be done manually
    load_text = f"""
    USING PERIODIC COMMIT 10000 
		LOAD CSV FROM "file:///rels/{meta_id}/{meta_id}.csv.gz" AS row FIELDTERMINATOR "," 
    WITH row
    MATCH
      (drug:Drug {{label: row[0]}}),
      (disease:Disease) where row[1] in disease.efo
    MERGE
      (drug)-
      [o:OPENTARGETS_DRUG_TO_DISEASE]
      ->(disease)
    RETURN
       count(o);
    """
    load_text = load_text.replace("\n", " ").replace("\t", " ")
    load_text = " ".join(load_text.split())

    create_constraints([load_text], meta_id)


if __name__ == "__main__":
    process()
