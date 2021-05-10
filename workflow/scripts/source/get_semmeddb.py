import os
import sys
import pandas as pd
import wget
from loguru import logger
from workflow.scripts.utils.general import copy_source_data

data_name = "semmeddb"

# load semmemd tab separated predication to filter and create smaller version
PREDICATION_FILE = sys.argv[1]
GENERIC_CONCEPT_FILE = sys.argv[2]

# data are downloaded from here https://ii.nlm.nih.gov/SemRep_SemMedDB_SKR/SemMedDB/SemMedDB_download.shtml
# due to the license requirements, there doesn't seem to be a way to download the data easily
# need to download the PREDICATION and CITATION CSV files

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


def process():
    df = pd.read_csv(PREDICATION_FILE, encoding="ISO-8859-1")
    col_names = [
        "predication_id",
        "sentence_id",
        "PMID",
        "predicate",
        "subject_id",
        "subject_name",
        "subject_type",
        "subject_novelty",
        "object_id",
        "object_name",
        "object_type",
        "object_novelty",
        "x",
        "y",
        "z",
    ]
    df.columns = col_names
    logger.info(f"\n{df.head()}")
    logger.info(df.shape)

    # filter on predicates
    df = df[~df.predicate.isin(predIgnore)]
    logger.info(df.shape)

    # filter on novelty columns
    # logger.info(f"\n{df['subject_novelty'].value_counts()}")
    # logger.info(f"\n{df['object_novelty'].value_counts()}")
    # subject_nov = df[df.subject_novelty==0]['subject_name'].value_counts()
    # object_nov = df[df.subject_novelty==0]['object_name'].value_counts()
    # logger.info(subject_nov)
    # logger.info(object_nov)

    # filter to only keep rows with both subject and object novelty of 1
    # df = df[(df['subject_novelty']==1) & (df['object_novelty']==1)]
    # logger.info(df.shape)
    # logger.info(f'\n{df.head()}')

    # use generic concept file instead of novelty columns
    gc_df = pd.read_csv(GENERIC_CONCEPT_FILE, names=["concept_id", "cui", "name"])
    gc_ids = list(gc_df["cui"])
    logger.info(gc_df.head())
    df = df[~(df["subject_id"].isin(gc_ids)) & ~(df["object_id"].isin(gc_ids))]
    logger.info(f"\n{df.head()}")
    logger.info(df.shape)

    # remove last three cols
    df.drop(columns=["x", "y", "z"], inplace=True)

    # write to file
    sem_name = os.path.basename(PREDICATION_FILE).split(".")[0]
    logger.info(sem_name)
    filename = f"/tmp/{sem_name}_filter.csv.gz"
    df.to_csv(filename, index=False)
    copy_source_data(data_name=data_name, filename=filename)


if __name__ == "__main__":
    process()
