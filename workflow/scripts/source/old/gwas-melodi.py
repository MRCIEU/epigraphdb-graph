import requests
import time
import json
import requests
import pandas as pd
import re
import multiprocessing as mp
from itertools import islice
import os
import gzip

api_url = 'https://melodi-presto.mrcieu.ac.uk/api/'

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
    print("get_gwas_data")
    gwas_api_url = "http://api.mrbase.org/get_studies?access_token=null"
    gwas_res = requests.get(gwas_api_url).json()

    # get filter list
    filterList = get_gwas_filter()

    gwasInfo = {}
    for g in gwas_res:
        if g["id"].startswith("UKB-b"):
            if g["id"] in filterList:
                gwasInfo[g["id"]] = g["trait"].replace("/", "_")
        else:
            gwasInfo[g["id"]] = g["trait"].replace("/", "_")

    print(len(gwasInfo), "gwasinfo")
    return gwasInfo


def get_gwas_data_v2():
    gwas_api_url = "http://gwasapi.mrcieu.ac.uk/gwasinfo"
    gwas_res = requests.get(gwas_api_url).json()
    # filterList = get_gwas_filter()
    gwasInfo = {}
    for g in gwas_res:
        gwasInfo[gwas_res[g]["id"]] = gwas_res[g]["trait"]
        #if gwas_res[g]["id"] == "ieu-a-1137":
            #gwasInfo[gwas_res[g]["id"]] = gwas_res[g]["trait"]
        #    gwasInfo[gwas_res[g]["id"]] = 'PCSK9'
    print(len(gwasInfo), "gwasinfo")
    #print(gwasInfo)
    return gwasInfo


def preprocess_traits(gwasInfo):
    processed = {}
    for g in gwasInfo:
        params = {"text_list": list(g["trait"]), "source": "ukbb"}
        process_res = requests.post(
            "http://vectology-api.mrcieu.ac.uk/preprocess", data=json.dumps(params)
        )
        processed[g] = process_res
    return processed


def enrich(gwasInfo, full):
    # get enriched triples for all GWAS
    print("enrich")
    url = api_url + "enrich/"
    headers = {"content-type": "application/x-www-form-urlencoded"}
    for i in gwasInfo:
        print(i)
        iTrait = gwasInfo[i]
        for regex in filter_list:
            r = re.compile(regex)
            mi = r.match(iTrait)
            if mi:
                iTrait = mi.group(1)
        params = {"query": iTrait}
        print(url, params)
        r = requests.post(url, data=json.dumps(params), headers=headers)
        #print(r.text)
        # df = pd.(json.dumps(r.text))
        try:
            #data_only = list(json.loads(r.text).values())[0]
            data_only = json.loads(r.text)
            #print(data_only)
            # df = list()
            df = pd.DataFrame.from_dict(data_only)
            print(df.head())
            df["gwas_id"] = i
            if df.size > 0:
                # df.iloc[:10,:5]
                df.to_csv("melodi/" + i + ".tsv", sep="\t", index=False)
        except:
            print("no data")


def compare_traits(subset, gwasInfo):
    testCount = 0
    headers = {"content-type": "application/x-www-form-urlencoded"}
    url = api_url + "overlap/"
    for i in subset:
        for j in gwasInfo:
            # check if alredy done
            fName = i.replace(":", "-") + "__" + j.replace(":", "-") + ".com.tsv.gz"
            if os.path.exists("compare/" + fName):
                print(fName, "already done")
                continue
            testCount += 1
            if i != j:
                if testCount < 1000000:
                    # filter ukb terms
                    iTrait = subset[i]
                    jTrait = gwasInfo[j]
                    for regex in filter_list:
                        r = re.compile(regex)
                        mi = r.match(iTrait)
                        mj = r.match(jTrait)
                        if mi:
                            iTrait = mi.group(1)
                        if mj:
                            jTrait = mj.group(1)
                    params = {"exposure": [iTrait], "outcome": [jTrait]}
                    print(url, params)
                    r = requests.post(url, data=json.dumps(params), headers=headers)
                    # fName=i.replace(' ','_')+':'+j.replace(' ','_')+'.com.tsv'
                    try:
                        df = pd.read_json(r.text)
                        if df.size > 0:
                            df.to_csv("compare/" + fName, sep="\t", compression="gzip")
                        else:
                            df.to_csv("compare/" + fName, sep="\t", compression="gzip")
                            print(fName, "empty")
                    except:
                        print(r.text)
                else:
                    break


def compare_sem_df(aList, aID, bList, bID):
    textbase_data = os.path.join(config.localPath, "textbase", "data")
    pValCut = 1e-1
    print("reading a data from", aList)
    all_a = []
    for a in aList:
        aName = os.path.join(textbase_data, a + ".gz")
        if os.path.exists(aName):
            df = pd.read_csv(aName, sep="\t")
            print(df.shape)
            # remove low pval
            df = df[df["pval"] < pValCut]
            print(df.shape)
            df["set"] = a
            df["id"] = aID
            all_a.append(df)
        else:
            print(a, "is missing from", textbase_data)
    if len(all_a) > 0:
        aframe = pd.concat(all_a, axis=0, ignore_index=True)
        print(aframe.shape)
    else:
        print("aframe is empty")

    print("reading b data from", bList)
    all_b = []
    for b in bList:
        bName = os.path.join(textbase_data, b + ".gz")
        if os.path.exists(bName):
            df = pd.read_csv(bName, sep="\t")
            # remove low pval
            df = df[df["pval"] < pValCut]
            df["set"] = b
            df["id"] = bID
            all_b.append(df)
        else:
            print(b, "is missing from", textbase_data)
    if len(all_b) > 0:
        bframe = pd.concat(all_b, axis=0, ignore_index=True)
        print(bframe.shape)
    else:
        print("bframe is empty")

    if len(all_a) > 0 and len(all_b) > 0:
        print("finding overlaps...")
        overlap = aframe.merge(bframe, left_on="object_name", right_on="subject_name")
        print(overlap.head())
        print(overlap.shape)
        # overlap.to_csv('o.txt',sep='\t')
        return {
            "count": overlap.shape[0],
            "data": json.loads(overlap.to_json(orient="records")),
        }
    else:
        return json.dumps({"error": "no overlaps"})


def compare_traits_local(subset, gwasInfo):
    print("comparing traits...")
    headerFile = "compare/gwas-melodi-header.tsv"
    o = open(headerFile, "w")
    header = [
        "triple_x",
        "subject_name_x",
        "subject_type_x",
        "subject_id_x",
        "predicate_x",
        "object_name_x",
        "object_type_x",
        "object_id_x",
        "localCount_x",
        "localTotal_x",
        "globalCount_x",
        "globalTotal_x",
        "odds_x",
        "pval_x",
        "pmids_x",
        "set_x",
        "id_x",
        "triple_y",
        "subject_name_y",
        "subject_type_y",
        "subject_id_y",
        "predicate_y",
        "object_name_y",
        "object_type_y",
        "object_id_y",
        "localCount_y",
        "localTotal_y",
        "globalCount_y",
        "globalTotal_y",
        "odds_y",
        "pval_y",
        "pmids_y",
        "set_y",
        "id_y",
    ]
    o.write("\t".join(header))
    o.close()
    for i in subset:
        for j in gwasInfo:
            # check if alredy done
            # fName=i.replace(':','-')+'__'+j.replace(':','-')+'.com.tsv.gz'
            # if os.path.exists('compare/'+fName):
            #    print(fName,'already done')
            #    continue
            if i != j:
                # filter ukb terms
                iTrait = subset[i]
                jTrait = gwasInfo[j]
                for regex in filter_list:
                    r = re.compile(regex)
                    mi = r.match(iTrait)
                    mj = r.match(jTrait)
                    if mi:
                        iTrait = mi.group(1)
                    if mj:
                        jTrait = mj.group(1)
                # add underscores
                iTrait = iTrait.replace(" ", "_").lower()
                jTrait = jTrait.replace(" ", "_").lower()
                # r = requests.post(url, data=json.dumps(params), headers=headers)
                r = compare_sem_df([iTrait], i, [jTrait], j)
                if "data" in r:
                    # fName=i.replace(' ','_')+':'+j.replace(' ','_')+'.com.tsv'
                    try:
                        # print(r)
                        df = pd.read_json(json.dumps(r["data"]))
                        print(df.head())
                        with open("compare/gwas-melodi-1e-1.tsv", "a") as f:
                            if df.size > 0:
                                # df.to_csv('compare/'+fName,sep='\t',compression='gzip')
                                df.to_csv(
                                    f,
                                    sep="\t",
                                    compression="gzip",
                                    header=False,
                                    index=False,
                                )
                            # else:
                            # df.to_csv('compare/'+fName,sep='\t',compression='gzip')
                            # print(fName,'empty')
                    except Exception as e:
                        print("Error", str(e))


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
    print("getting gwas info")
    gwasInfo = get_gwas_data_v2()

    # create test set
    gwasInfoTest = {k: gwasInfo[k] for k in list(gwasInfo)[:10]}
    gwasInfoTest = gwasInfo
    print(len(gwasInfoTest))
    # print('processing traits')
    # processedInfo = preprocess_traits(gwasInfo)

    # enrich in parallel
    gwasChunks = chunks(gwasInfoTest,10)
    pool = mp.Pool(processes=10)
    results = pool.starmap(enrich, [(gwasData,gwasInfoTest) for gwasData in gwasChunks])
    pool.close()

    # takes 34 minutes for 11k GWAS and 4.5k output
    # takes 167 minutes for 31k GWAS and 2.1GB output

    # create single file
    # for i in melodi/*; do tail -n +2 $i; done | gzip > gwas-melodi-enrich.tsv.gz

    # enrich(gwasInfoTest)
    # compare_traits_local(gwasInfoTest)
    # print('Deleting old data...')
    # try:
    #    os.remove('compare/gwas-melodi*')
    # except OSError:
    #    print('No file to delete')

    # this is not needed if data going into a graph

    # gwasChunks = chunks(gwasInfoTest,10)
    # pool = mp.Pool(processes=10)
    # results = pool.starmap(compare_traits_local, [(gwasData,gwasInfoTest) for gwasData in gwasChunks])
    # pool.close()


# text_post()
# orcid_post()
melodi_gwas()
