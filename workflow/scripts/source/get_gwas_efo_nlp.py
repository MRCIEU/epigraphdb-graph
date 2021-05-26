import requests
import pandas as pd
import time
import os
import numpy as np
from scipy.spatial.distance import cosine
from scipy.spatial import distance
from loguru import logger
from workflow.scripts.utils.general import copy_source_data, get_data_from_server
import json
import gzip
import re

# get all trait data from GWAS API
# process text and get text embedding using vectology API
# create distance data by comparing all to all
# takes about 15 minutes to run on 30,000 traits

data_name = "EFO"
EFO_data = "efo_nodes_2021-05-04.csv"
tmp_dir = "/tmp/epigraph-build/EFO"
timestr = time.strftime("%Y%m%d")


filter_list = [
    "^Blood clot, DVT, bronchitis, emphysema, asthma, rhinitis, eczema, allergy diagnosed by doctor: (.*)",
    "^Cancer code.*?self-reported:\s(.*)",
    "^Contributory (secondary) causes of death: ICD10:\s.*?\s(.*)",
    "^Diagnoses - main ICD10:\s.*?\s(.*)",
    "^Diagnoses - main ICD9:\s.*?\s(.*)",
    "^Diagnoses - secondary ICD10:\s.*?\s(.*)",
    "^Diagnoses - secondary ICD9:\s.*?\s(.*)",
    "^External causes:\s.*?\s(.*)",
    "^Eye problems/disorders: (.*)",
    "^Medication for cholesterol.*?blood pressure.*?diabetes.*?or take exogenous hormones: (.*)",
    "^Medication for cholesterol.*?blood pressure or diabetes: (.*)",
    "^Medication for pain relief.*?constipation.*?heartburn.*?: (.*)",
    "^Medication for smoking cessation, constipation, heartburn, allergies \(pilot\): (.*)",
    "^Non-cancer illness code.*?self-reported:\s(.*)",
    "^Operation code: (.*)",
    "^Operative procedures - main OPCS:\s.*?\s(.*)",
    "^Operative procedures - secondary OPCS:\s.*?\s(.*)",
    "^Treatment/medication code: (.*)",
    "^Type of cancer: ICD10:\s.*?\s(.*)",
    "(.*?)\(.*?\)$",
]

def get_gwas_traits():
    logger.info("Getting gwas data")
    gwas_api_url = "http://gwasapi.mrcieu.ac.uk/gwasinfo"
    logger.info(gwas_api_url)
    gwas_res = requests.get(gwas_api_url).json()
    gwasInfo = {}
    for g in gwas_res:
        if not gwas_res[g]["id"].startswith("eqtl"):
            t = gwas_res[g]['trait']
            # filter 
            for regex in filter_list:
                r = re.compile(regex)
                mi = r.match(t)
                if mi:
                    t = mi.group(1)
            # deal with general parentheses
            t = re.sub(r'\([^)]*\)', '', t)
            #logger.info(t)
            gwasInfo[gwas_res[g]["id"]] = t
    logger.info("gwasinfo len {}", len(gwasInfo))
    gwas_df = pd.DataFrame.from_dict(gwasInfo, orient="index", columns=["value"])
    # gwas_df = gwas_df[gwas_df[]]
    logger.info(gwas_df.head())
    logger.info(gwas_df.shape)
    return gwas_df


# function to get filtered text
def filter_text(textList):
    url = "http://vectology-api.mrcieu.ac.uk/preprocess"
    payload = {"text_list": textList, "source": "ukbb"}
    response = requests.post(url, data=json.dumps(payload))
    res = response.json()
    return res


# function to get embedding
def embed_text(textList, model):
    url = "http://vectology-api.mrcieu.ac.uk/encode"
    payload = {"text_list": textList, "model_name": model}
    response = requests.post(url, data=json.dumps(payload))
    res = response.json()
    return res["embeddings"]


def get_gwas_embeddings():
    f = "/tmp/ieu-gwas-embeddings-" + str(timestr) + ".pkl"
    if os.path.exists(f):
        logger.info(f"{f} already done")
        gwas_df = pd.read_pickle(f)
    else:
        gwas_df = get_gwas_traits()
        logger.info("Filtering text and creating emebeddings...")
        embeddingList = []
        filterList = []
        counter = 0
        for k, g in gwas_df.groupby(np.arange(len(gwas_df)) // 50):
            if counter % 20 == 0:
                logger.info(counter * 50)
            counter += 1
            textList = list(g["value"].values)
            filteredTextList = []
            filteredRes = filter_text(textList)
            for i in filteredRes:
                filteredTextList.append(i["result"])
            filterList.extend(filteredTextList)
            embeddings = embed_text(filteredTextList, "BioSentVec")
            # add to dictionary
            for i in range(0, len(filteredTextList)):
                # ignore empty vectors
                # if np.count_nonzero(embeddings[i])>0:
                embeddingList.append(embeddings[i])
                # vectorDic[k]={'text':textList[i],'embedding':embeddings[i]}
        logger.info("{} embeddings done", len(embeddingList))
        gwas_df["embedding"] = embeddingList
        # add filtertext
        gwas_df["filter"] = filterList
        gwas_df.to_pickle(f)
    logger.info(gwas_df.shape)
    return gwas_df


def get_efo_embeddings():
    f = "/tmp/efo-embeddings-" + str(timestr) + ".pkl"
    f = "/tmp/efo-embeddings-20210510.pkl"
    if os.path.exists(f):
        logger.info(f"{f} already done")
        efo_df = pd.read_pickle(f)
    else:
        get_data_from_server(dataName=f"EFO/{EFO_data}", outDir=tmp_dir)
        efo_df = pd.read_csv(f"{tmp_dir}/{EFO_data}")
        logger.info(efo_df.head())
        counter = 0
        efo_list = []
        embeddingList = []
        for k, g in efo_df.groupby(np.arange(len(efo_df)) // 50):
            if counter % 20 == 0:
                logger.info(counter * 50)
            counter += 1
            efo_list = list(g["lbl"].values)
            embeddings = embed_text(efo_list, "BioSentVec")
            # add to dictionary
            for i in range(0, len(efo_list)):
                # ignore empty vectors
                # if np.count_nonzero(embeddings[i])>0:
                embeddingList.append(embeddings[i])
                # vectorDic[k]={'text':textList[i],'embedding':embeddings[i]}
        logger.info("{} embeddings done", len(embeddingList))
        efo_df["embedding"] = embeddingList
        # add filtertext
        efo_df.to_pickle(f)
    logger.info(efo_df.shape)
    return efo_df


def create_distances(gwas_df, efo_df):
    logger.info("Creating distances...")

    score_cutoff = 0.7
    filename = f"{tmp_dir}/gwas-efo-cosine-{timestr}-{score_cutoff}.tsv.gz"
    if os.path.exists(filename):
        logger.info(f"{filename} done")
    else:
        o = gzip.open(filename, "wt")

        gwas_vectors = np.array(list(gwas_df["embedding"]))
        gwas_ids = gwas_df.index.tolist()

        efo_vectors = np.array(list(efo_df["embedding"]))
        efo_ids = list(efo_df["id"])

        pws = distance.cdist(gwas_vectors, efo_vectors, metric="cosine")
        logger.info(len(pws))

        logger.info("Writing to file...")
        for i in range(0, len(gwas_ids)):
            for j in range(i, len(efo_ids)):
                if i != j:
                    # logger.info(f'{gwas_ids[i]} {efo_ids[j]} {pws[i][j]}')
                    score = 1 - pws[i][j]
                    if score > score_cutoff:
                        t = f"{gwas_ids[i]}\t{efo_ids[j]}\t{str(score)}\n"
                        o.write(t)
        o.close()
    copy_source_data(data_name, filename)


if __name__ == "__main__":
    #get_gwas_traits()
    gwas_df = get_gwas_embeddings()
    efo_df = get_efo_embeddings()
    create_distances(gwas_df, efo_df)
