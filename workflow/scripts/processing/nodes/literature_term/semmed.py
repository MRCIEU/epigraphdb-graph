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

# todo
# deal with name case issues, e.g.
# Ellagic Acid,C0013900,145,phsu;orch,SemMedDB_VER42_2020_R
# ellagic acid,C0013900,2,phsu,SemMedDB_VER42_2020_R
# fixed by capitalizing .str.capitalize()

# bollocks!
# Arachis oil,C0052300,13,phsu,SemMedDB_VER42_2020_R
# Peanut oil,C0052300,4,orch,SemMedDB_VER42_2020_R


def process():
    # load predicate data
    logger.info("loading data {}", FILE)
    df = pd.read_csv(
        os.path.join(dataDir, FILE), sep=",", compression="gzip"
    )
    logger.info(df.shape)
    logger.info(df.head())

    # create series of subject/object names
    term_names = pd.concat([df["subject_name"], df["object_name"]])
    # create series of subject/object types
    term_types = pd.concat([df["subject_type"], df["object_type"]])
    # create series of subject/object ids
    term_ids = pd.concat([df["subject_id"], df["object_id"]])
    # create new df
    term_df = pd.concat([term_names, term_types, term_ids], axis=1)
    term_df.columns = ["name", "type", "id"]

    # split ids and names by |
    subject_ids = term_df.id.str.split("|")
    subject_names = term_df.name.str.split("|")
    # create dictionary of ids to names
    dic_list = list(map(dict, map(zip, subject_ids, subject_names)))
    id_name_dic = {}
    for d in dic_list:
        id_name_dic.update(d)

    # split the IDs by | onto new rows
    term_df = (
        term_df.assign(id=term_df.id.str.split("|"))
        .explode("id")
        .reset_index(drop=True)
    )
    logger.info(term_df.shape)
    logger.info("\n {}", term_df)

    # some ids have multiple types - need to create a list of types for each ID
    # make a map of id to types
    id_type_dic = term_df.groupby(["id"])["type"].apply(list).to_dict()
    # make lists unique
    for i in id_type_dic:
        l = ";".join(list(set(id_type_dic[i])))
        id_type_dic[i] = l

    # annoyingly, ids can have multiple names too - so do the same
    ###note - this has been avoided by id-name dictionary above
    # make a map of id to names
    # id_name_dic=term_df.groupby( ['id'] )['name'].apply(list).to_dict()
    # make lists unique
    # for i in id_name_dic:
    #    l = ";".join(list(set(id_name_dic[i])))
    #    id_name_dic[i]=l

    # create counts by id
    #term_df = pd.DataFrame({"count": term_df.groupby(["id"]).size()}).reset_index()
    
    # add type lists back as ; separated array
    term_df["type"] = term_df["id"].map(id_type_dic)
    logger.info("\n {}", term_df)
    # add names back
    term_df["name"] = term_df["id"].map(id_name_dic)
    logger.info("\n {}", term_df)
    term_df.drop_duplicates(inplace=True)
    create_import(df=term_df, meta_id=args.name)

    # create constraints
    constraintCommands = [
        "CREATE CONSTRAINT ON (s:LiteratureTerm) ASSERT s.id IS UNIQUE",
        "match (l:LiteratureTerm ) match (g:Gene) where toLower(g.name) = toLower(l.name) merge (l)-[:TERM_TO_GENE]->(g) return count(g);",
    ]
    create_constraints(constraintCommands, meta_id)


if __name__ == "__main__":
    process()
