"""
Retrieve clinvar data
"""

import datetime
import os
import wget
import pandas as pd
from workflow.scripts.utils import settings
from workflow.scripts.utils.general import copy_source_data
from workflow.scripts.utils.general import neo4j_connect
from biomart import BiomartServer

data_name = "clinvar"

env_configs = settings.env_configs
data_dir = os.path.join(env_configs["data_dir"], "clinvar")

# data_dir = os.path.join("data_folder", "clinvar")  # local test
# os.makedirs(data_dir, exist_ok=True)

today = datetime.date.today()

# define file names: can't be csv (commas in vars)
clinvar_data_file = os.path.join(data_dir, f"clinvar_gene_condition_source_id-{today}.tsv")
clinvar_gene_condition_mapping = os.path.join(data_dir, f"clinvar_gene_condition_source_id-{today}_tidy_subset.tsv")
clinvar_gene_condition_mapping_mondo = os.path.join(data_dir,
                                                    f"clinvar_gene_condition_source_id-{today}_tidy_subset_MONDO.tsv")
clinvar_gene_condition_mapping_umls = os.path.join(data_dir,
                                                    f"clinvar_gene_condition_source_id-{today}_tidy_subset_MONDO_via_UMLS.tsv")

# file that will be used in the local mode
local_file = os.path.join(data_dir, "gene_condition_source_id.tsv")


def download_data():
    link = 'https://ftp.ncbi.nlm.nih.gov/pub/clinvar/gene_condition_source_id'
    wget.download(link, clinvar_data_file)
    #copy_source_data(data_name=data_name, filename=clinvar_data_file)


def load_data(file):
    df = pd.read_csv(file, sep='\t').drop_duplicates()
    return df


def collapse_gene_groups(df):
    # collapse associated and related genes into a new column
    gene_types_list = []
    gene_names_all = []

    for ind in df.index:
        if pd.notna(df['AssociatedGenes'][ind]):
            gene_names_all.append(df['AssociatedGenes'][ind])
            gene_types_list.append("Associated gene")
        elif pd.notna(df['RelatedGenes'][ind]):
            gene_names_all.append(df['RelatedGenes'][ind])
            gene_types_list.append("Related gene")

    # Create a column from the list
    df['Gene name'] = gene_names_all
    df['GeneType'] = gene_types_list

    # Drop redundant columns
    df = df.drop(columns=["AssociatedGenes", "RelatedGenes"])
    return df


def gene_name_to_ensembl_id():
    atts = ['external_gene_name', 'ensembl_gene_id']
    # server = BiomartServer( "http://www.ensembl.org/biomart" ) # latest:  not using to keep everything in sync with build 37
    server = BiomartServer("http://grch37.ensembl.org/biomart")
    hge = server.datasets['hsapiens_gene_ensembl']

    # collect data from server as list of lists
    s = hge.search({'attributes': atts}, header=1)
    biomart_list = list()
    for l in s.iter_lines():
        line = l.decode("utf-8").split("\t")
        biomart_list.append(line)

    # convert to pandas df and fix the colnames
    biomart_table = pd.DataFrame(biomart_list)
    biomart_table.columns = biomart_table.iloc[0]
    biomart_table = biomart_table.drop([0, ]).drop_duplicates()
    return biomart_table


def subset_genes(df):
    # get gene name to ensembl id mapping from biomart
    biomart_table = gene_name_to_ensembl_id()
    # subset clinvar data to only genes that have ensembl id
    df_subset = df.merge(biomart_table, on='Gene name', how='inner')
    return df_subset


def make_tidy_clinvar_output(df):
    # make tidy dates
    df['LastUpdated'] = pd.to_datetime(df['LastUpdated']).dt.strftime('%Y-%m-%d')

    # make tidy mondo ids
    for ind in df.index:
        if df['SourceName'][ind] == "MONDO":
            mondo_id = str(df['SourceID'][ind].split(":")[1])
            mondo_url = "http://purl.obolibrary.org/obo/MONDO_" + mondo_id
            df['SourceID'][ind] = mondo_url

    # subset and rename columns
    df = df[["Gene name", "Gene stable ID", "GeneType",
             "DiseaseName", "ConceptID",
             "SourceName", "SourceID", "DiseaseMIM",
             'LastUpdated']]
    df.columns = ["gene_name", "ensembl_id", "clinvar_gene_type",
                  "disease_name", "umls_id",
                  "source_name", "source_id", "disease_MIM",
                  "last_updated"]
    df.to_csv(clinvar_gene_condition_mapping, sep="\t", index=False)
    #copy_source_data(data_name=data_name, filename=clinvar_gene_condition_mapping)


def make_gene_to_mondo_map():
    # subset to records with mondo id and select only required columns
    df = pd.read_csv(clinvar_gene_condition_mapping, sep='\t')
    df = df[df["source_name"] == "MONDO"]
    df = df[['ensembl_id', 'source_id', 'clinvar_gene_type', 'last_updated']]
    df.to_csv(clinvar_gene_condition_mapping_mondo, sep="\t", index=False)
    #copy_source_data(data_name=data_name, filename=clinvar_gene_condition_mapping_mondo)


def query_graph(query):
    # collect to epigraph
    driver = neo4j_connect()
    session = driver.session()
    # query
    query_data = session.run(query).data()
    df = pd.json_normalize(query_data)
    return df


def make_gene_to_umls_to_mondo_map():
    df = pd.read_csv(clinvar_gene_condition_mapping, sep='\t')
    df = df[df["umls_id"].notna()]
    df = df[['ensembl_id', 'umls_id', 'clinvar_gene_type', 'last_updated']]

    # query epigraph by umls to get mondo ids and disease labels
    ulms = list(set(df["umls_id"]))
    query = """
        match (d:Disease)
        where any(umls in {} WHERE umls IN d.umls)
        return d.label, d.umls, d.id
        """.format(ulms)

    query_df = query_graph(query)
    query_df.columns = ["disease_label", "umls_id", "mondo_id"]

    # unfold umls lists into separate rows
    query_df = query_df.explode("umls_id")

    # join clinvar table with query output to map UMLS to mondo id and labels
    df_joined = df.merge(query_df, on='umls_id', how='inner')
    df_joined = df_joined[['ensembl_id', 'mondo_id', 'disease_label', 'umls_id', 'clinvar_gene_type', 'last_updated']]

    df_joined.to_csv(clinvar_gene_condition_mapping_umls, sep="\t", index=False)
    # copy_source_data(data_name=data_name, filename=clinvar_gene_condition_mapping_umls)


if __name__ == "__main__":
    download_data()
    data_raw = load_data(clinvar_data_file)
    #data_raw = load_data(local_file)

    data = collapse_gene_groups(data_raw)
    data_subset = subset_genes(data)
    make_tidy_clinvar_output(data_subset)

    make_gene_to_mondo_map()
    make_gene_to_umls_to_mondo_map()