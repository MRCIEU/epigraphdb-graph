import ast

import pandas as pd

def convert_list_cols(df: pd.DataFrame) -> pd.DataFrame:
    col_names = df.columns
    for col in ["evidence", "publication"]:
        if col in col_names:
            df[col] = df[col].apply(ast.literal_eval)
            df[col].head()
    return df
