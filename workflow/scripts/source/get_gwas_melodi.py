import requests
import time
import datetime
import subprocess
import json
import requests
import pandas as pd
import re
import multiprocessing as mp
from itertools import islice
import os
import gzip
from loguru import logger
from workflow.scripts.utils.general import copy_source_data

api_url = "https://melodi-presto.mrcieu.ac.uk/api/"
os.makedirs("melodi", exist_ok=True)
today = datetime.date.today()
data_name = "melodi"

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


def get_gwas_filter():
    filterList = []
    filterFile = "data/ukb_id_keep.txt"
    with open(filterFile, "r") as f:
        for line in f:
            filterList.append(line.rstrip())
    return filterList


def get_gwas_data():
    gwas_api_url = "http://gwasapi.mrcieu.ac.uk/gwasinfo"
    gwas_res = requests.get(gwas_api_url).json()
    # filterList = get_gwas_filter()
    gwasInfo = {}
    for g in gwas_res:
        gwasInfo[gwas_res[g]["id"]] = gwas_res[g]["trait"]
        # if gwas_res[g]["id"] == "ieu-a-1137":
        # gwasInfo[gwas_res[g]["id"]] = gwas_res[g]["trait"]
        #    gwasInfo[gwas_res[g]["id"]] = 'PCSK9'
    logger.info("gwasinfo {}", len(gwasInfo))
    return gwasInfo


def enrich(gwasInfo, full):
    # get enriched triples for all GWAS
    logger.info("enrich")
    url = api_url + "enrich/"
    headers = {"content-type": "application/x-www-form-urlencoded"}
    for i in gwasInfo:
        logger.info(i)
        if i.startswith("eqtl"):
            continue
        iTrait = gwasInfo[i]
        for regex in filter_list:
            r = re.compile(regex)
            mi = r.match(iTrait)
            if mi:
                iTrait = mi.group(1)
        params = {"query": iTrait}
        logger.info("{} {}", url, params)
        r = requests.post(url, data=json.dumps(params), headers=headers)
        # df = pd.(json.dumps(r.text))
        try:
            # data_only = list(json.loads(r.text).values())[0]
            data_only = json.loads(r.text)
            # df = list()
            df = pd.DataFrame.from_dict(data_only)
            df["gwas_id"] = i
            logger.info(df)
            if df.size > 0:
                # df.iloc[:10,:5]
                df.to_csv(f"melodi/{i}.tsv", sep="\t", index=False)
        except:
            logger.warning("no data")


def chunks(data, SIZE=100):
    it = iter(data)
    for i in range(0, len(data), SIZE):
        yield {k: data[k] for k in islice(it, SIZE)}


def melodi_gwas():
    # gwasInfo={'1':'leptin','2':'adiponectin'}
    # gwasInfo={'1':'pcsk9','2':'adiponectin'}
    # gwasInfo = {
    #    "1":"Operative procedures - main OPCS: E03.5 Incision of septum of nose",
    #    "2":"Type of cancer: ICD10: C83.7 Burkitt's tumour",
    #    "3":"Type of cancer: ICD10: C92.0 Acute myeloid leukaemia",
    # }
    logger.info("getting gwas info")
    gwasInfo = get_gwas_data()

    # create test set
    gwasInfoTest = {k: gwasInfo[k] for k in list(gwasInfo)[:10]}
    logger.info(len(gwasInfoTest))
    # gwasInfo = gwasInfoTest

    # enrich in parallel
    gwasChunks = chunks(gwasInfo, 10)
    pool = mp.Pool(processes=10)
    results = pool.starmap(enrich, [(gwasData, gwasInfo) for gwasData in gwasChunks])
    pool.close()

    # create single file
    filename = f"gwas-melodi-enrich-{today}.tsv.gz"
    com = f"for i in melodi/*; do tail -n +2 $i; done | gzip > {filename}"
    subprocess.call(com, shell=True)
    copy_source_data(data_name=data_name, filename=filename)

    # takes 34 minutes for 11k GWAS and 4.5k output
    # takes 167 minutes for 31k GWAS and 2.1GB output


if __name__ == "__main__":
    melodi_gwas()
