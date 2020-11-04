from ontoma import OnToma
import os
import requests
import pandas as pd
import json

print("Creating otmap...")
otmap = OnToma()

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


def get_gwas_data():
    gwas_file = "data/gwas-data.tsv"
    print("Getting IEU GWAS data...")
    if os.path.exists(gwas_file):
        df = pd.read_csv(gwas_file, sep="\t")
        print("Done")
    else:
        # create the data
        gwas_api_url = "http://gwasapi.mrcieu.ac.uk/gwasinfo"
        gwas_res = requests.get(gwas_api_url).json()
        outData = open(gwas_file, "w")
        df = pd.DataFrame(gwas_res)
        df = df.T.fillna("")
        print(df.head())
        print(df["year"].describe())
        df.to_csv(outData, sep="\t", index=False, header=True)
        outData.close()
        print("Done")
    return df


df = get_gwas_data()

outFile = open("data/ontoma.json", "w")
for i, row in df.iterrows():
    gwas_trait = row["trait"]
    filtered_gwas_text = filter_text([gwas_trait])[0]["result"]
    print("\n", filtered_gwas_text)
    try:
        o = otmap.find_term(filtered_gwas_text, verbose=True)
        if o:
            o["id"] = row["id"]
            print(row["id"] + "\t" + str(o))
            outFile.write(json.dumps(o) + "\n")
    except:
        print("ontoma error")
