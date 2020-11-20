import os
import pandas as pd
from loguru import logger

#################### leave me heare please :) ########################

from workflow.scripts.utils.general import setup, get_source
from workflow.scripts.utils.writers import create_import

# setup
args, dataDir = setup()
meta_id = args.name

# args = the argparse arguments (name and data)
# dataDir = the path to the working directory for this node/rel

#######################################################################

FILE = get_source(meta_id, 1)

def process():
    df = pd.read_csv(os.path.join(dataDir, FILE), sep="\t")
    print(df.head())
    print(df.shape)
    df.columns = ["source", "target"]
    logger.info(df.shape)
    logger.info("\n {}", df.head())
    create_import(df=df, meta_id=meta_id)
    
if __name__ == "__main__":
    process()
