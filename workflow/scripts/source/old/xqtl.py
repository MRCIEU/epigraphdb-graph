from pathlib import Path
from typing import Tuple

import numpy as np
import pandas as pd
from loguru import logger

# Params
LOAD_CSV_DIR = Path("~/data/import").expanduser()
DATA_DIR = Path("~/data/export").expanduser()
XQTL_DIR = LOAD_CSV_DIR / "xqtl"
OUTPUT_DIR = DATA_DIR / "xqtl"
# data files
EQTL_MR_SINGLE_MRB = XQTL_DIR / "eQTL-MR-single-mrb.txt"
EQTL_MR_SINGLE_SAIGE = XQTL_DIR / "eQTL-MR-sigle-saige.txt"
PQTL = XQTL_DIR / "pQTL-mrb-saige.single.final.v2.txt"


def clean_eqtl(df: pd.DataFrame) -> pd.DataFrame:

    # Remove rows with non-finite standard errors
    logger.info("Remove rows with missing pvals")
    with pd.option_context("mode.use_inf_as_null", True):
        df = df[~df["se"].isna()]

    # outcome:
    # - keep only trait ids for now
    # - replace outcome name with corresponding trait ids
    logger.info("Subset outcome")
    df = (
        df[df["outcome"].astype(str).apply(lambda x: " || " in x)]
        .assign(outcome=lambda df: df["id.outcome"])
        .drop(["id.exposure", "id.outcome", "samplesize"], axis=1)
    )

    # Take only rows with smallest pvalues within each group of
    # exposure-outcome-SNP
    logger.info("Get smallest pvals")
    df = (
        df.sort_values(["p"], ascending=True)
        .groupby(["exposure", "outcome", "SNP"])
        .head(1)
        .reset_index(drop=True)
    )

    return df


def clean_pqtl(df: pd.DataFrame) -> pd.DataFrame:

    # Remove rows with non-finite standard errors
    logger.info("Remove rows with missing pvals")
    with pd.option_context("mode.use_inf_as_null", True):
        df = df[~df["se"].isna()]

    # outcome:
    # - keep only trait ids for now
    # - replace outcome name with corresponding trait ids
    logger.info("Subset outcome")
    df = (
        df[df["outcome"].astype(str).apply(lambda x: " || " in x)]
        .assign(outcome=lambda df: df["id.outcome"])
        .drop(["id.exposure", "id.outcome", "samplesize"], axis=1)
    )

    # Take only rows with smallest pvalues within each group of
    # exposure-outcome-SNP
    logger.info("Get smallest pvals")
    df = (
        df.sort_values(["p"], ascending=True)
        .groupby(["exposure", "outcome", "SNP"])
        .head(1)
        .reset_index(drop=True)
    )

    logger.info("process exposure")
    # Remove the particular cases in pqtl of missing exposure names
    df = df[~df["exposure"].isna()]
    # Remove the cases of gene id combinations
    gene_id = "ENSG00000135346;ENSG00000104826"
    df = df[df["exposure"] != gene_id]

    return df


def clean_xqtl(
    df_eqtl: pd.DataFrame, df_pqtl: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df_xqtl = pd.concat(
        [df_eqtl.assign(qtl_type="eQTL"), df_pqtl.assign(qtl_type="pQTL")]
    )
    # Create rsid column from SNP
    df_xqtl = df_xqtl.assign(
        rsid=lambda df: df["SNP"].apply(lambda x: x if x[:2] == "rs" else np.nan),
        mr_method=lambda df: df["SNP"].apply(lambda x: "SNP" if x[:2] == "rs" else x),
    ).drop("SNP", axis=1)

    logger.info("df_xqtl info")
    df = df_xqtl.groupby(["qtl_type", "mr_method"]).count()
    print(df)

    # split into df_xqtl_single_snp and df_xqtl_multi_snp
    df_xqtl_single_snp = df_xqtl[df_xqtl["mr_method"] == "SNP"].drop(
        "mr_method", axis=1
    )

    df_xqtl_multi_snp = df_xqtl[
        df_xqtl["mr_method"].isin(["All - Inverse variance weighted", "All - MR Egger"])
    ].drop("rsid", axis=1)
    df_xqtl_multi_snp.loc[
        df_xqtl_multi_snp["mr_method"] == "All - Inverse variance weighted", "mr_method"
    ] = "IVW"
    df_xqtl_multi_snp.loc[
        df_xqtl_multi_snp["mr_method"] == "All - MR Egger", "mr_method"
    ] = "Egger"

    # Reset index to row id and rename to "id"
    df_xqtl_single_snp = df_xqtl_single_snp.reset_index(drop=True)
    df_xqtl_single_snp.index = df_xqtl_single_snp.index.rename("id")
    df_xqtl_multi_snp = df_xqtl_multi_snp.reset_index(drop=True)
    df_xqtl_multi_snp.index = df_xqtl_multi_snp.index.rename("id")

    df = df_xqtl_single_snp.groupby("qtl_type").count()
    logger.info(f"df_xqtl_single_snp info\n{df}")

    df = df_xqtl_multi_snp.groupby(["qtl_type", "mr_method"]).count()
    logger.info(f"df_xqtl_multi_snp info\n{df}")

    return df_xqtl_single_snp, df_xqtl_multi_snp


def harmonise_xqtl_multi_snp_mr(df) -> pd.DataFrame:
    df = (
        df[["exposure", "outcome", "b", "se", "p", "qtl_type", "mr_method"]]
        .rename(
            columns={
                "exposure": "Gene.ensembl_id",
                "outcome": "Gwas.id",
                "b": "XQTL_MULTI_SNP_MR.b",
                "se": "XQTL_MULTI_SNP_MR.se",
                "p": "XQTL_MULTI_SNP_MR.p",
                "qtl_type": "XQTL_MULTI_SNP_MR.qtl_type",
                "mr_method": "XQTL_MULTI_SNP_MR.mr_method",
            }
        )
        .drop_duplicates()
    )
    return df


def harmonise_xqtl_single_snp_mr_snp_gene(df) -> pd.DataFrame:
    df = (
        df[["rsid", "exposure"]]
        .rename(columns={"rsid": "Snp.name", "exposure": "Gene.ensembl_id"})
        .drop_duplicates()
    )
    return df


def harmonise_xqtl_single_snp_mr_gene_gwas(df) -> pd.DataFrame:
    df = (
        df[["exposure", "outcome", "b", "se", "p", "qtl_type", "rsid"]]
        .rename(
            columns={
                "exposure": "Gene.ensembl_id",
                "outcome": "Gwas.id",
                "b": "XQTL_SINGLE_SNP_MR_GENE_GWAS.b",
                "se": "XQTL_SINGLE_SNP_MR_GENE_GWAS.se",
                "p": "XQTL_SINGLE_SNP_MR_GENE_GWAS.p",
                "qtl_type": "XQTL_SINGLE_SNP_MR_GENE_GWAS.qtl_type",
                "rsid": "XQTL_SINGLE_SNP_MR_GENE_GWAS.rsid",
            }
        )
        .drop_duplicates()
    )
    return df


def main() -> None:
    # Do nothing about EQTL_MR_SINGLE_SAIGE right now
    df_eqtl_mrb = pd.read_csv(EQTL_MR_SINGLE_MRB, sep="\t")
    df_pqtl = pd.read_csv(PQTL, sep="\t")

    df_eqtl = clean_eqtl(df_eqtl_mrb)
    df_pqtl = clean_pqtl(df_pqtl)

    df_xqtl_single_snp, df_xqtl_multi_snp = clean_xqtl(df_eqtl=df_eqtl, df_pqtl=df_pqtl)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df_xqtl_single_snp.to_csv(OUTPUT_DIR / "xqtl_single_snp.csv", index=False)
    df_xqtl_multi_snp.to_csv(OUTPUT_DIR / "xqtl_multi_snp.csv", index=False)

    xqtl_multi_snp_mr = harmonise_xqtl_multi_snp_mr(df_xqtl_multi_snp)
    xqtl_single_snp_mr_snp_gene = harmonise_xqtl_single_snp_mr_snp_gene(
        df_xqtl_single_snp
    )
    xqtl_single_snp_mr_gene_gwas = harmonise_xqtl_single_snp_mr_gene_gwas(
        df_xqtl_single_snp
    )

    xqtl_multi_snp_mr.to_csv(OUTPUT_DIR / "XQTL_MULTI_SNP_MR.csv", index=False)
    xqtl_single_snp_mr_snp_gene.to_csv(
        OUTPUT_DIR / "XQTL_SINGLE_SNP_MR_SNP_GENE.csv", index=False
    )
    xqtl_single_snp_mr_gene_gwas.to_csv(
        OUTPUT_DIR / "XQTL_SINGLE_SNP_MR_GENE_GWAS.csv", index=False
    )
    return None


if __name__ == "__main__":
    main()
