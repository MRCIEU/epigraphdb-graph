import subprocess
import pandas as pd
import os
import datetime
import requests

from workflow.scripts.utils import settings
from workflow.scripts.utils.general import get_data_from_server, copy_source_data
from loguru import logger

env_configs = settings.env_configs

data_dir = "/tmp/epigraph-build/vep"
data_name = 'vep'
os.makedirs(data_dir,exist_ok=True)

today = datetime.date.today()

variant_data = [
    {
        'name':'opengwas',
        'file':'opengwas/opengwas-tophits-2020-10-13.csv',
        'snp_col':'rsid'
    },
    {
        'name':'mr-eve',
        'file':'mr-eve/21_03_10/variants.csv.gz',
        'snp_col':'variantId:ID(variant)'
    },
    {
        'name':'xqtl',
        'file': 'xqtl-processed/xqtl_single_snp.csv',
        'snp_col':'rsid'
    }
]

# setup
# vep docker image needs setting up - note volumes need to be same for setup and run
# docker run -t -i -v /data/vep_data:/opt/vep/.vep ensemblorg/ensembl-vep perl INSTALL.pl -a cf -s homo_sapiens -y GRCh37

vep_data_dir='/data/vep_data'

v_file=f'{vep_data_dir}/variants-{today}.txt'

def get_existing():
    url = 'https://api.epigraphdb.org/cypher'
    data = {'query': 'MATCH (v:Variant)-[r:VARIANT_TO_GENE]-(g:Gene) RETURN distinct v._id as variant_id'}
    r = requests.post(url, json=data)
    r.raise_for_status()
    res = r.json()['results']
    logger.info(len(res))
    df = pd.DataFrame(res)
    logger.info(df.head())
    return df

def get_data():
    for i in variant_data:
        get_data_from_server(dataName=i['file'],outDir=data_dir)

def process_variants():
    variant_df=pd.DataFrame(columns = ['rsid'])
    for i in variant_data:
        logger.info(i)
        logger.info(f"processing {i['name']}")
        f = os.path.basename(i['file'])
        df = pd.read_csv(f"{data_dir}/{f}",low_memory=False)
        df = df[i['snp_col']]
        df.drop_duplicates(inplace=True)
        logger.info(df.head())
        variant_df = pd.concat([variant_df,df])
        logger.info(variant_df.shape)
    variant_df.drop_duplicates(inplace=True)
    variant_df.to_csv(v_file,index=False,header=False,sep=' ')

def run_vep(variant_file):
    com="""
        docker run -t -i -v {vep_data_dir}:/opt/vep/.vep 
        ensemblorg/ensembl-vep ./vep --port 3337 --cache --fork 20 --assembly GRCh37 
        -i /opt/vep/.vep/{variant_file} 
        -o /opt/vep/.vep/vep-{today}.txt 
        --per_gene 
        --no_intergenic
    """.format(vep_data_dir=vep_data_dir,variant_file=variant_file,today=today)
    com = com.replace('\n',' ')
    logger.info(com)
    subprocess.call(com, shell=True)
    
    #copy results 
    copy_source_data(data_name=data_name, filename=f"/data/vep_data/vep-{today}.txt")

    #com = f"cp /data/vep_data/vep-{today}.txt {env_configs['data_dir']}/vep/"
    #subprocess.call(com, shell=True)

if __name__ == "__main__":
    if os.path.exists(v_file):
        logger.info(f'{v_file} exists')
    else:
        existing_df = get_existing()
        get_data()
        process_variants()
    run_vep(os.path.basename(v_file))
    #run_vep('variants-2021-05-06_head.txt')
    