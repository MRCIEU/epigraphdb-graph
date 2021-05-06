import subprocess
import pandas as pd
import os
import datetime
import requests

from workflow.scripts.utils import settings
from workflow.scripts.utils.general import get_data_from_server
from loguru import logger

env_configs = settings.env_configs

data_dir = "/tmp/epigraph-build/vep"
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
        'file':'opengwas/opengwas-tophits-2020-10-13.csv',
        'snp_col':'rsid'
    },
    #{}
]

# setup
# vep docker image needs setting up - note volumes need to be same for setup and run
# docker run -t -i -v /data/vep_data:/opt/vep/.vep ensemblorg/ensembl-vep perl INSTALL.pl -a cf -s homo_sapiens -y GRCh37

vep_data_dir='/data/vep_data'

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

def process_variants(variant_data):
    for i in variant_data:
        logger.info(i)
        logger.info(f"processing {i['name']}")
        df = pd.read_csv(i['file'],low_memory=False)
        df = df[i['snp_col']]
        df.drop_duplicates(inplace=True)
        logger.info(df.head())
    #df.head.to_csv(f'{vep_data_dir}/variants-{today}.txt',index=False,header=False)

def run_vep(variant_dir,variant_file):
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
    com = f"cp /data/vep_data/vep-{today}.txt {env_configs['data_dir']}/vep/"
    subprocess.call(com, shell=True)

if __name__ == "__main__":
    variant_data = [
        {
        'name':'opengwas',
        'file':os.path.join(env_configs['data_dir'],'opengwas','opengwas-tophits-2020-10-13.csv'),
        'snp_col':'rsid'
        }
    ]
    #existing_df = get_existing()
    #process_variants(variant_data)
    #run_vep('/tmp',f'variants-{today}.txt')
    get_data()