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
# this isn't ideal, but need to 
Lit_data = [
    "neo4j/1.1/import/nodes/lit-term-semmed/lit-term-semmed.csv.gz",
    "neo4j/1.1/import/nodes/literature-term-semrep-biorxiv/literature-term-semrep-biorxiv.csv.gz",
    "neo4j/1.1/import/nodes/literature-term-semrep-medrxiv/literature-term-semrep-medrxiv.csv.gz",
]
tmp_dir = "/tmp/epigraph-build/EFO"
timestr = time.strftime("%Y%m%d")

# function to get embedding
def embed_text(textList, model):
    url = "http://vectology-api.mrcieu.ac.uk/encode"
    payload = {"text_list": textList, "model_name": model}
    response = requests.post(url, data=json.dumps(payload))
    res = response.json()
    return res["embeddings"]

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

def get_lit_embeddings():
    f = "/tmp/lit-embeddings-" + str(timestr) + ".pkl"
    if os.path.exists(f):
        logger.info(f"{f} already done")
        lit_df = pd.read_pickle(f)
    else:
        #get_data_from_server(dataName=f"/{Lit_data}", outDir=tmp_dir)
        #lit_df = pd.read_csv(f"{tmp_dir}/{Lit_data}")
        #logger.info(lit_df.head())
        df1 = pd.read_csv(Lit_data[0],names=['name','type','id','source','_name','_id'])
        df2 = pd.read_csv(Lit_data[1],names=['name','type','id','source','_name','_id'])
        df3 = pd.read_csv(Lit_data[2],names=['name','type','id','source','_name','_id'])
        lit_df = pd.concat([df1,df2,df3])
        logger.info(f'\n{lit_df.head()}')
        logger.info(lit_df.shape)
        lit_df = lit_df['name']
        lit_df.drop_duplicates(inplace=True)
        logger.info(lit_df.shape)

        counter = 0
        lit_list = []
        embeddingList = []
        for k, g in lit_df.groupby(np.arange(len(lit_df)) // 50):
            if counter % 20 == 0:
                logger.info(counter * 50)
            counter += 1
            lit_list = list(g.values)
            embeddings = embed_text(lit_list, "BioSentVec")
            # add to dictionary
            for i in range(0, len(lit_list)):
                # ignore empty vectors
                # if np.count_nonzero(embeddings[i])>0:
                embeddingList.append(embeddings[i])
                # vectorDic[k]={'text':textList[i],'embedding':embeddings[i]}
        logger.info("{} embeddings done", len(embeddingList))
        lit_df["embedding"] = embeddingList
        # add filtertext
        lit_df.to_pickle(f)
    logger.info(lit_df.shape)
    return lit_df

def create_distances(efo_df, lit_df):
    logger.info("Creating distances...")

    score_cutoff = 0.7
    filename = f"{tmp_dir}/efo-lit-cosine-{timestr}-{score_cutoff}.tsv.gz"
    if os.path.exists(filename):
        logger.info(f"{filename} done")
    else:
        o = gzip.open(filename, "wt")

        lit_vectors = np.array(list(lit_df["embedding"]))
        lit_ids = lit_df.index.tolist()

        efo_vectors = np.array(list(efo_df["embedding"]))
        efo_ids = list(efo_df["id"])

        pws = distance.cdist(gwas_vectors, efo_vectors, metric="cosine")
        logger.info(len(pws))

        logger.info("Writing to file...")
        for i in range(0, len(lit_ids)):
            for j in range(i, len(efo_ids)):
                if i != j:
                    # logger.info(f'{gwas_ids[i]} {efo_ids[j]} {pws[i][j]}')
                    score = 1 - pws[i][j]
                    if score > score_cutoff:
                        t = f"{lit_ids[i]}\t{efo_ids[j]}\t{str(score)}\n"
                        o.write(t)
        o.close()
    copy_source_data(data_name, filename)


if __name__ == "__main__":
    lit_df = get_lit_embeddings()
    #efo_df = get_efo_embeddings()
    #create_distances(efo_df,lit_df)
