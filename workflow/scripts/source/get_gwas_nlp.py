import requests
import pandas as pd
import time
import numpy as np
from scipy.spatial.distance import cosine
from scipy.spatial import distance
from loguru import logger
from workflow.scripts.utils.general import copy_source_data
import json
import gzip

# get all trait data from GWAS API
# process text and get text embedding using vectology API
# create distance data by comparing all to all
# takes about 15 minutes to run on 30,000 traits

data_name = "trait-nlp"

def get_gwas_traits():
    logger.info("Getting gwas data")
    gwas_api_url = "http://gwasapi.mrcieu.ac.uk/gwasinfo"
    logger.info(gwas_api_url)
    gwas_res = requests.get(gwas_api_url).json()
    gwasInfo = {}
    for g in gwas_res:
        gwasInfo[gwas_res[g]["id"]] = gwas_res[g]["trait"]
    logger.info("gwasinfo len {}",len(gwasInfo))
    gwas_df = pd.DataFrame.from_dict(gwasInfo, orient="index", columns=["value"])
    logger.info(gwas_df.head())
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


def get_embedding(gwas_df):
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
    logger.info("{} embeddings done",len(embeddingList))
    gwas_df["embedding"] = embeddingList
    # add filtertext
    gwas_df["filter"] = filterList
    timestr = time.strftime("%Y%m%d")
    gwas_df.to_csv(
        "/tmp/ieu-gwas-embeddings-" + str(timestr) + ".tsv.gz",
        sep="\t",
        compression="gzip",
    )
    return gwas_df


def create_distances(gwas_df):
    logger.info("Creating distances...")
    # https://stackoverflow.com/questions/48838346/how-to-speed-up-computation-of-cosine-similarity-between-set-of-vectors

    vectors = []
    ids = []

    for i, j in gwas_df.iterrows():
        vectors.append(j["embedding"])
        ids.append(i)

    timestr = time.strftime("%Y%m%d")
    score_cutoff = 0
    filename = f'/tmp/ieu-gwas-cosine-{timestr}-{score_cutoff}.tsv.gz'
    o = gzip.open(filename, "wt")

    logger.info(len(vectors))
    data = np.array(vectors)
    pws = distance.pdist(data, metric="cosine")
    logger.info(len(pws))
    logger.info(len(ids))

    logger.info("Writing to file...")
    mCount = 0
    for i in range(0, len(ids)):
        for j in range(i, len(ids)):
            if i != j:
                # print(ids[i],ids[j],1-pws[mCount])
                score = 1 - pws[mCount]
                if score > score_cutoff:
                    t = f"{ids[i]}\t{ids[j]}\t{str(score)}\n"
                    o.write(t)
                mCount += 1
    o.close()
    logger.info(mCount)
    copy_source_data(data_name, filename)

if __name__ == "__main__":
    gwas_info = get_gwas_traits()
    gwas_df = get_embedding(gwas_info)
    create_distances(gwas_df)
