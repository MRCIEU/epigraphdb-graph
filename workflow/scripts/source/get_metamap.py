import requests
import datetime
import pandas as pd
import time
import os
import numpy as np
import json
import glob
import sys

from workflow.scripts.utils.general import copy_source_data

data_name = "metamap"
today = datetime.date.today()

# create the files
# python -m workflow.scripts.source.get_metamap create
# run metamap in parallel (on SSD machine)
# find ./data/metamap/sep-traits/ -name "*.txt" | parallel -j 20 /data/software/metamap-lite/public_mm_lite/metamaplite.sh  --segment_lines {}
# tidy up the files
# python -m workflow.scripts.source.get_metamap process

# create output directory
meta_dir = "data/metamap/sep-traits/"
if not os.path.exists(meta_dir):
    os.makedirs(meta_dir)


def get_gwas_data():
    print("Getting gwas data...")
    gFile = "./data/gwas-data.tsv"
    if os.path.exists(gFile):
        print("Already done")
        df = pd.read_csv(gFile, sep="\t")
    else:
        # create the data
        gwas_api_url = "http://gwasapi.mrcieu.ac.uk/gwasinfo"
        gwas_res = requests.get(gwas_api_url).json()
        outData = open(gFile, "w")
        df = pd.DataFrame(gwas_res)
        df = df.T.fillna("")
        print(df.head())
        print(df["year"].describe())
        df.to_csv(outData, sep="\t", index=False)
        outData.close()
    return df


# function to get filtered text
def filter_text(textList):
    url = "http://vectology-api.mrcieu.ac.uk/preprocess"
    payload = {"text_list": textList, "source": "ukbb"}
    # print(payload)
    response = requests.post(url, data=json.dumps(payload))
    # print(response)
    res = response.json()
    # print(res)
    return res


def create_sep_files_for_metamap(df):
    for i, row in df.iterrows():
        gwas_id = row["id"]
        o = open(os.path.join(meta_dir, gwas_id + ".txt"), "w")
        trait = row["filter"].replace("'", "").lower().rstrip()
        o.write(trait)
        o.close()


def create_data_set(gwas_df):
    print("Filtering text...")
    embeddingList = []
    filterList = []
    counter = 0
    for k, g in gwas_df.groupby(np.arange(len(gwas_df)) // 50):
        if counter % 10 == 0:
            print(counter * 50)
        counter += 1
        textList = list(g["trait"].values)
        # print(textList)
        filteredTextList = []
        filteredRes = filter_text(textList)
        for i in filteredRes:
            filteredTextList.append(i["result"])
        filterList.extend(filteredTextList)

    # add filtertext
    gwas_df["filter"] = filterList
    return gwas_df


def create_metamap_data(dir):
    filename = os.path.join(dir, f"metamap-data-{today}.tsv")
    o2 = open(filename, "w")
    # os.chdir(dir)
    for file in glob.glob(os.path.join(dir, "sep-traits", "*.mmi")):
        trait_id = file.split(".")[0]
        # get trait text
        t = open(os.path.join(trait_id + ".txt"), "r").read().rstrip()
        # print(t)
        with open(os.path.join(file)) as f:
            for line in f:
                l = line.rstrip().split("|")
                # print(l)
                # print(trait_id,l[2],l[4],l[8])
                if float(l[2]) > 1:
                    o2.write(
                        trait_id.split("/")[-1]
                        + "\t"
                        + t
                        + "\t"
                        + l[2]
                        + "\t"
                        + l[3]
                        + "\t"
                        + l[4]
                        + "\t"
                        + l[8]
                        + "\n"
                    )
    o2.close()
    copy_source_data(data_name=data_name, filename=filename)


if __name__ == "__main__":
    if sys.argv[1] == "create":
        gwas_info = get_gwas_data()
        gwas_df = create_data_set(gwas_info)
        create_sep_files_for_metamap(gwas_df)
    elif sys.argv[1] == "process":
        create_metamap_data("data/metamap")
    else:
        print("create or process")
        exit()
