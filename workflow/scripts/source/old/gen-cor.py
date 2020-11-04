import requests
import os
import pandas as pd
import config
from elasticsearch import Elasticsearch

# takes 45 minutes

es = Elasticsearch([{"host": config.oracle_es_host, "port": config.oracle_es_port}])


def elastic_search(index_name, gwas_id):
    filterData = []
    filterData.append({"term": {"gwas_id": gwas_id}})
    filterData.append({"range": {"p": {"lt": 1e-8}}})
    # print(index_name,filterData)
    res = es.search(
        request_timeout=60,
        index=index_name,
        # doc_type="assoc",
        body={
            "size": "1000",
            "sort": [{"p": "asc"}],
            "query": {"bool": {"filter": filterData}},
        },
    )
    # print(res['hits']['total'])
    return res


def es_top_hits(gwas_id, index_name):
    print("Getting top GWAS hits", gwas_id, index_name)
    o = open("./data/top-snps/" + index_name + "-" + gwas_id + ".tsv", "w")
    res = elastic_search(index_name, gwas_id)
    hits = res["hits"]["hits"]
    for hit in hits:
        # print hit['_source']
        snp = hit["_source"]["snp_id"]
        pval = hit["_source"]["p"]
        if hit["_source"]["beta"] != "":
            beta = int(hit["_source"]["beta"]) * -1
        else:
            beta = ""
        o.write(snp + "\t" + str(pval) + "\n")
    o.close()


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


def get_top_hits():
    print("Getting top hits...")
    # set top snp number
    topSnps = "1000"
    traitLimit = "100000"
    chunkSize = 10

    gwas_df = get_gwas_data()
    # print(studyData)
    chunk = 1
    for i, row in gwas_df.iterrows():
        print("\n####", i, "####")
        i1, i2, gwas_id = row["id"].split("-")
        index_name = i1 + "-" + i2
        print(index_name, gwas_id)
        es_top_hits(gwas_id=gwas_id, index_name=index_name)


# get_gwas_data()
get_top_hits()
