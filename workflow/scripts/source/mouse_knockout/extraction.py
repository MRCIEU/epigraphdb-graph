import argparse
import math
from pathlib import Path
from typing import Callable, Dict, List

import requests
import pandas as pd
from loguru import logger
from tqdm.contrib.concurrent import process_map

API = "https://www.alliancegenome.org"
DISEASE_DATA = Path("data") / "disease_df.csv"
GENE_OUTPUT = Path("data") / "diseases_to_genes.csv"
ALLELE_OUTPUT = Path("data") / "diseases_to_alleles.csv"
MODEL_OUTPUT = Path("data") / "diseases_to_models.csv"
NUM_TRIALS = 50
NUM_WORKERS = 4


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="Run the script but no output to write to.",
    )
    parser.add_argument(
        "--trial",
        action="store_true",
        help="A trial run",
    )
    parser.add_argument(
        "-j",
        "--num-workers",
        default=NUM_WORKERS,
        type=int,
        help="Num multi proc workers",
    )
    return parser


def extract(func: Callable, doid_list: List[str], num_workers: int = NUM_WORKERS):
    chunksize = math.floor(len(doid_list) / num_workers)
    map_res = process_map(func, doid_list, max_workers=num_workers, chunksize=chunksize)
    df = pd.concat(map_res)
    return df


def animal_disease_to_genes(doid: str) -> pd.DataFrame:
    def _extract(entry: Dict) -> Dict:
        # process lists
        publications = entry["publications"]
        # evidence is unique, as it appears to be
        evidence = entry["evidenceCodes"]
        res = {
            # Disease
            "primary_disease_id": doid,
            # NOTE: the specific disease might differ from the primary disease id,
            #       which could be a child term
            "disease_id": entry["disease"]["id"],
            # Gene
            "gene_id": entry["gene"]["id"],  # HGNC id
            "gene_name": entry["gene"]["symbol"],  # e.g. ACE2
            "gene_species_id": entry["gene"]["species"]["taxonId"],  # taxid id
            "gene_species_name": entry["gene"]["species"]["name"],  # e.g. Homo sapiens
            # REL
            "association_type": entry["associationType"],  # e.g. is_implicated_in
            # Evidence, NOTE: nested
            "evidence": evidence,
            # Publication
            "publication_id": [_["id"] for _ in publications],
        }
        return res

    route = "/api/disease/{id}/genes"
    r = requests.get(API + route.format(id=doid))
    r.raise_for_status()
    results = r.json()["results"]
    df = pd.json_normalize([_extract(_) for _ in results])
    return df


def animal_disease_to_alleles(doid: str) -> pd.DataFrame:
    def _extract(entry: Dict) -> Dict:
        # process lists
        publications = entry["publications"]
        # evidence is unique, as it appears to be
        evidence = entry["evidenceCodes"]
        res = {
            # Disease
            "primary_disease_id": doid,
            "disease_id": entry["disease"]["id"],
            # Allele
            "allele_id": entry["allele"]["id"],  # MGI id
            "allele_name": entry["allele"]["symbolText"],  # e.g. "Ace2<em1(ACE2)Yowa>"
            "allele_species_id": entry["allele"]["species"]["taxonId"],  # taxid id
            "allele_species_name": entry["allele"]["species"][
                "name"
            ],  # e.g. Homo sapiens
            # REL
            "association_type": entry["associationType"],  # e.g. is_implicated_in
            # Evidence, NOTE: nested
            "evidence": evidence,
            # Publication
            "publication_id": [_["id"] for _ in publications],
        }
        return res

    route = "/api/disease/{id}/alleles"
    r = requests.get(API + route.format(id=doid))
    r.raise_for_status()
    results = r.json()["results"]
    df = pd.json_normalize([_extract(_) for _ in results])
    return df


def animal_disease_to_models(doid: str) -> pd.DataFrame:
    def _extract(entry: Dict) -> Dict:
        # process lists
        publications = entry["publications"]
        # evidence is unique, as it appears to be
        evidence = entry["evidenceCodes"]
        res = {
            # Disease
            "primary_disease_id": doid,
            "disease_id": entry["disease"]["id"],
            # Model
            "model_id": entry["model"]["id"],  # MGI id
            "model_name": entry["model"]["nameText"],
            "model_species_id": entry["model"]["species"]["taxonId"],  # taxid id
            "model_species_name": entry["model"]["species"][
                "name"
            ],  # e.g. Homo sapiens
            # REL
            "association_type": entry["associationType"],  # e.g. is_implicated_in
            # Evidence, NOTE: nested
            "evidence": evidence,
            # Publication
            "publication_id": [_["id"] for _ in publications],
        }
        return res

    route = "/api/disease/{id}/models"
    r = requests.get(API + route.format(id=doid))
    r.raise_for_status()
    results = r.json()["results"]
    df = pd.json_normalize([_extract(_) for _ in results])
    return df


def main():
    parser = create_parser()
    args = parser.parse_args()
    print(args)

    assert DISEASE_DATA.exists()
    disease_df = pd.read_csv(DISEASE_DATA)
    if args.trial:
        disease_df = disease_df[:NUM_TRIALS]

    if args.dry_run:
        exit()

    # genes
    doid_list = disease_df["DOID"].tolist()
    funcs = [
        animal_disease_to_genes,
        animal_disease_to_alleles,
        animal_disease_to_models,
    ]
    outputs = [GENE_OUTPUT, ALLELE_OUTPUT, MODEL_OUTPUT]
    for func, output in zip(funcs, outputs):
        logger.info(f"Processing on {func}")
        df = extract(func=func, doid_list=doid_list, num_workers=args.num_workers)
        logger.info(df.head())
        logger.info(f"Write to {output}")
        df.to_csv(output, index=False)

    # NOTE: columns `evidence` and `publication` are lists
    # and when reading need to convert using ast.literal_eval.


if __name__ == "__main__":
    main()
