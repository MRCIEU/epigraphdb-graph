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

FILE = get_source(meta_id, 1)


def process():
    data = os.path.join(dataDir, FILE)
    # some bad rows caused by extra commas so need to skip
    df = pd.read_csv(data, sep=",", skiprows=1, error_bad_lines=False)
    keep_cols = [
        "Gene",
        "Drug",
        "Guideline",
        "CPIC Level",
        "PharmGKB Level of Evidence",
        "PGx on FDA Label",
    ]
    df = df[keep_cols]
    df.drop_duplicates(inplace=True)
    df.columns = [
        "target",
        "source",
        "guideline",
        "cpic_level",
        "pharmgkb_level_of_evidence",
        "pgx_on_fda_label",
    ]
    # set label to uppercase
    df["source"] = df["source"].str.upper()
    logger.info(df.shape)
    logger.info("\n {}", df.head())
    create_import(df=df, meta_id=meta_id, import_type="load")

    load_text = f"""
		USING periodic commit 10000 
		load CSV from "file:///rels/{meta_id}/{meta_id}.csv.gz" as row FIELDTERMINATOR "," 
		WITH row, coalesce(row[2],"NA") as guideline_data, coalesce(row[3],"NA") as level_data, coalesce(row[4],"NA") as pharmgkb_data, coalesce(row[5],"NA") as pgx_data 
		match  (g:Gene{{name:row[0]}}) match (d:Drug{{label:row[1]}})
		merge (g)<-[c:CPIC{{guideline:guideline_data, cpic_level:level_data, pharmgkb_level_of_evidence:pharmgkb_data, pgx_on_fda_label:pgx_data,_source:["CPIC"]}}]-(d)
        return count(g);
		"""
    load_text = load_text.replace("\n", " ").replace("\t", " ")
    load_text = " ".join(load_text.split())

    create_constraints([load_text], meta_id)


if __name__ == "__main__":
    process()
