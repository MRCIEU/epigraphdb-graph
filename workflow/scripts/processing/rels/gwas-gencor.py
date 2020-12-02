import os
import re
import gzip
import json
import sys
import csv
import pandas as pd
import requests

from loguru import logger

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

def get_gwas_data():
    gwas_api_url = "http://gwasapi.mrcieu.ac.uk/gwasinfo"
    logger.info("Getting gwas data from {}", gwas_api_url)
    gwas_res = requests.get(gwas_api_url).json()
    df = pd.DataFrame(gwas_res)
    df = df.T.fillna("")
    logger.info(df.shape)
    #only want uk biobank traits
    df = df[df['id'].str.startswith('ukb-')]
    logger.info(df.shape)
    logger.info("\n {}",df.head())
    return df

def process(gwas_df):
    logger.info("Reading {}", FILE)
    gc_df = pd.read_csv(os.path.join(dataDir, FILE), delimiter=",")
    logger.info(gc_df.head())
    logger.info(gc_df.shape)
    regex = r".*?<a.*?>(.*?)</a>.*"
    n1=[]
    n2=[]
    for values in gc_df['Phenotype 1']:
        name = re.search(regex,values)
        n1.append(name.group(1))
    for values in gc_df['Phenotype 2']:
        name = re.search(regex,values)
        n2.append(name.group(1))
    gc_df['phenotype_name_1']=n1
    gc_df['phenotype_name_2']=n2
    logger.info("\n{}",gc_df)
    logger.info(gc_df.shape)
    
    #merge with opengwas df
    merge_df = pd.merge(gc_df,gwas_df[['id','trait']],left_on="phenotype_name_1",right_on="trait",how='left')
    merge_df = merge_df.rename(columns={'id':'phenotype_1_id'})
    merge_df.drop('trait',inplace=True, axis=1)

    merge_df = pd.merge(merge_df,gwas_df[['id','trait']],left_on="phenotype_name_2",right_on="trait",how='left')
    merge_df = merge_df.rename(columns={'id':'phenotype_2_id'})
    merge_df.drop(['trait','ID1','ID2','Phenotype 1','Phenotype 2','phenotype_name_1','phenotype_name_2'],inplace=True, axis=1)

    logger.info("\n{}",merge_df)
    logger.info(merge_df.shape)

    merge_df = merge_df.rename(columns={"phenotype_1_id": "source", "phenotype_2_id": "target"})
    create_import(df=merge_df, meta_id=meta_id)

if __name__ == "__main__":
    df = get_gwas_data()
    process(df)
