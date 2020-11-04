import requests
import pandas as pd
import time
from elasticsearch import Elasticsearch
import numpy as np
from sklearn.metrics import pairwise_distances
from scipy.spatial.distance import cosine
from scipy.spatial import distance
import json

# get all trait data from GWAS API
# process text and get text embedding using vectology API
# create distance data by comparing all to all
# takes about 15 minutes to run on 30,000 traits


def get_gwas_traits():
    print("Getting gwas data")
    gwas_api_url = "http://gwasapi.mrcieu.ac.uk/gwasinfo"
    print(gwas_api_url)
    gwas_res = requests.get(gwas_api_url).json()
    gwasInfo = {}
    for g in gwas_res:
        gwasInfo[gwas_res[g]["id"]] = gwas_res[g]["trait"]
    print(len(gwasInfo), "gwasinfo")
    gwas_df = pd.DataFrame.from_dict(gwasInfo, orient="index", columns=["value"])
    print(gwas_df.head())
    return gwas_df


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


# function to get embedding
def embed_text(textList, model):
    url = "http://vectology-api.mrcieu.ac.uk/encode"
    payload = {"text_list": textList, "model_name": model}
    # print(payload)
    response = requests.post(url, data=json.dumps(payload))
    # print(response)
    res = response.json()
    # print(res)
    return res["embeddings"]


def get_embedding(gwas_df):
    print("Filtering text and creating emebeddings...")
    embeddingList = []
    filterList = []
    counter = 0
    for k, g in gwas_df.groupby(np.arange(len(gwas_df)) // 50):
        if counter % 20 == 0:
            print(counter * 50)
        counter += 1
        textList = list(g["value"].values)
        # print(textList)
        filteredTextList = []
        filteredRes = filter_text(textList)
        for i in filteredRes:
            filteredTextList.append(i["result"])
        filterList.extend(filteredTextList)
        # print(filteredTextList)
        embeddings = embed_text(filteredTextList, "BioSentVec")
        # add to dictionary
        for i in range(0, len(filteredTextList)):
            # ignore empty vectors
            # if np.count_nonzero(embeddings[i])>0:
            embeddingList.append(embeddings[i])
            # vectorDic[k]={'text':textList[i],'embedding':embeddings[i]}
    print(len(embeddingList), "embeddings done")
    gwas_df["embedding"] = embeddingList
    # add filtertext
    gwas_df["filter"] = filterList
    timestr = time.strftime("%Y%m%d")
    gwas_df.to_csv(
        "output/ieu-gwas-embeddings-" + str(timestr) + ".tsv.gz",
        sep="\t",
        compression="gzip",
    )
    return gwas_df


def create_distances(gwas_df):
    print("Creating distances...")
    # https://stackoverflow.com/questions/48838346/how-to-speed-up-computation-of-cosine-similarity-between-set-of-vectors

    vectors = []
    ids = []

    for i, j in gwas_df.iterrows():
        vectors.append(j["embedding"])
        ids.append(i)

    timestr = time.strftime("%Y%m%d")
    o = open("output/ieu-gwas-cosine-" + str(timestr) + ".tsv.gz", "w")

    print(len(vectors))
    data = np.array(vectors)
    pws = distance.pdist(data, metric="cosine")
    print(len(pws))
    print(len(ids))

    print("Writing to file...")
    mCount = 0
    for i in range(0, len(ids)):
        for j in range(i, len(ids)):
            if i != j:
                # print(ids[i],ids[j],1-pws[mCount])
                score = 1 - pws[mCount]
                if score > 0.1:
                    o.write(ids[i] + "\t" + ids[j] + "\t" + str(score) + "\n")
                mCount += 1
    o.close()
    print(mCount)


if __name__ == "__main__":
    gwas_info = get_gwas_traits()
    gwas_df = get_embedding(gwas_info)
    create_distances(gwas_df)
