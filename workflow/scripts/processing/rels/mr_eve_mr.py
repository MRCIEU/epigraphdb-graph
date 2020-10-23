import os
import re
import gzip
import json
import sys
import csv
import pandas as pd

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

def process_data():
    print("Processing mr data...")
    col_names = [
        "source",
        "target",
        "method",
        "nsnp",
        "b",
        "se",
        "ci_low",
        "ci_upp",
        "pval",
        "selection",
        "moescore",
    ]
    csv_data = []
    for filename in os.listdir(dataDir):
        if filename.startswith("mrmoe") and filename.endswith(".csv.gz"):
            print(filename)
            fShort = os.path.splitext(os.path.basename(filename))[0]
            with gzip.open(os.path.join(dataDir, filename), "rt") as f:
                next(f)
                filereader = csv.reader(f, delimiter=",")
                for line in filereader:
                    (
                        id1,
                        id2,
                        method,
                        nsnp,
                        b,
                        se,
                        ci_low,
                        ci_upp,
                        pval,
                        selection,
                        moescore,
                    ) = line
                    # skip rows with missing pval or se
                    try:
                        float(pval)
                        float(se)
                    except ValueError:
                        continue
                    if not id1.startswith("UKB"):
                        id1 = "IEU-a-" + id1
                    if not id2.startswith("UKB"):
                        id2 = "IEU-a-" + id2
                    new_line = [
                        id1.replace(":", "-").lower(),
                        id2.replace(":", "-").lower(),
                        method,
                        nsnp,
                        b,
                        se,
                        ci_low,
                        ci_upp,
                        pval,
                        selection,
                        moescore,
                    ]
                    csv_data.append(new_line)

    # create csv file
    df = pd.DataFrame(csv_data)
    df.columns = col_names
    df.drop_duplicates(inplace=True)
    print(df.head())
    create_import(df=df, meta_id=meta_id)

    # constraints
    constraintCommands = [
        "match (g:Gwas)-[mr:MR_EVE_MR]->(g2:Gwas) set mr.log10pval = round(-log10(mr.pval));"
    ]
    create_constraints(constraintCommands, meta_id)


if __name__ == "__main__":
    process_data()
