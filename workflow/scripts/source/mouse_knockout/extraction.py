from typing import Dict

import requests
import pandas as pd

API = "https://www.alliancegenome.org"


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
