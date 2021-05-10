import os
import sys
import csv
import json
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


def create_ontoDic():
    ontoDic = {"doid": [], "umls": [], "efo": [], "icd9": [], "icd10": [], "mesh": []}
    return ontoDic


def create_ontology_properties(ontoList):
    ontoDic = create_ontoDic()
    if len(ontoDic) > 0:
        for i in ontoList:
            ont, val = i.split(":", 1)
            if ont.lower() in ontoDic:
                ontoDic[ont.lower()].append(val)
    for i in ontoDic:
        if len(ontoDic[i]) == 0:
            # set to empty to remove property from Neo4j
            ontoDic[i] = ""
    return ontoDic


def process():
    print("Processing")
    csv_data = []
    print("Reading")
    masterOnt = create_ontoDic()
    with open(os.path.join(dataDir, FILE)) as json_file:
        data = json.load(json_file)
        for d in data["graphs"][0]["nodes"]:
            # only CLASS types
            mondo_type = d["type"]
            if mondo_type != "CLASS":
                continue
            # skip deprecated
            if "meta" in d:
                if "deprecated" in d["meta"]:
                    if d["meta"]["deprecated"] == True:
                        continue
            # logger.debug(d)
            if "lbl" not in d:
                continue
            mondo_id = d["id"]
            mondo_label = d["lbl"]
            definition = "NA"
            xrefs = []
            ontoDic = create_ontoDic()
            if "meta" in d:
                if "definition" in d["meta"]:
                    definition = (
                        d["meta"]["definition"]["val"]
                        .replace('"', "")
                        .replace("\n", " ")
                    )
                if "xrefs" in d["meta"]:
                    for x in d["meta"]["xrefs"]:
                        xrefs.append(x["val"])
                    for i in xrefs:
                        ont, val = i.split(":", 1)
                        if ont.lower() in masterOnt:
                            masterOnt[ont.lower()].append(
                                {"val": val, "mondo": mondo_id}
                            )
                    ontoDic = create_ontology_properties(xrefs)
            # print(mondo_id,mondo_label,definition,xrefs)
            # print(list(ontoDic.values()))
            oList = [";".join(o) for o in list(ontoDic.values())]
            # print("\t".join(oList))
            d = [mondo_id, mondo_label, definition]
            d.extend(oList)
            csv_data.append(d)

    # print(masterOnt)
    for m in masterOnt:
        o = open(os.path.join(dataDir, m + ".csv"), "w")
        for i in masterOnt[m]:
            o.write(i["val"] + "," + i["mondo"] + "\n")
        o.close()

    # create csv file
    df = pd.DataFrame(csv_data)

    col_names = ["id", "label", "definition"]
    ontoLabels = list(ontoDic.keys())
    col_names.extend(ontoLabels)
    df.columns = col_names
    print(df.head())
    create_import(df=df, meta_id=meta_id)

    # create the constraints and indexes
    constraintCommands = [
        "CREATE CONSTRAINT ON (d:Disease) ASSERT d.id IS UNIQUE",
        "CREATE index on :Disease(label);",
        "CREATE index on :Disease(doid);",
    ]
    create_constraints(constraintCommands, meta_id)


def link():
    # map to ontologies
    load_text = []
    # efo
    load_text.append(
        f"""
        USING PERIODIC COMMIT 10000 
		LOAD CSV FROM "file:///nodes/{args.name}/efo.csv" AS row FIELDTERMINATOR "," 
		WITH row 
		MATCH (e:Efo) where e.id = "http://www.ebi.ac.uk/efo/EFO_"+row[0] MATCH (d:Disease) where d.id = row[1] 
		MERGE (e)<-[:MONDO_MAP_EFO{{_source:"Mondo"}}]-(d) return count(e) as efo_count
        """
    )
    # umls
    load_text.append(
        f"""
        USING PERIODIC COMMIT 10000 
		LOAD CSV FROM "file:///nodes/{args.name}/umls.csv" AS row FIELDTERMINATOR "," 
		WITH row 
		MATCH (s:LiteratureTerm) where s.id = row[0] MATCH (d:Disease) where d.id = row[1] 
		MERGE (s)<-[:MONDO_MAP_UMLS{{_source:"Mondo"}}]-(d) return count(s) as umls_count
        """
    )

    # umls using text
    load_text.append(
        f"""
        MATCH 
            (l:LiteratureTerm) 
        MATCH
            (d:Disease) 
        WHERE 
            toLower(l.name) = toLower(d.label) 
        MERGE 
            (l)<-[:MONDO_MAP_UMLS{{_source:"Mondo"}}]-(d)
        RETURN
            count(d)
        """
    )

    load_text = [t.replace("\n", " ").replace("\t", " ") for t in load_text]
    # load_text = " ".join(load_text.split())

    create_constraints(load_text, args.name)


if __name__ == "__main__":
    process()
    link()
