import os

from workflow.scripts.utils.general import copy_source_data

data_name = "string"

# uniprot mapping
uniprot_file = '/tmp/human.uniprot_2_string.2018.tsv.gz'
url = 'https://string-db.org/mapping_files/uniprot/human.uniprot_2_string.2018.tsv.gz'
os.system(f"wget -O {uniprot_file} {url}")
copy_source_data(data_name=data_name, filename=uniprot_file)

# string data
pp_file = '/tmp/9606.protein.links.v11.0.txt.gz'
url = 'https://stringdb-static.org/download/protein.links.v11.0/9606.protein.links.v11.0.txt.gz'
os.system(f"wget -O {pp_file} {url}")
copy_source_data(data_name=data_name, filename=pp_file)
