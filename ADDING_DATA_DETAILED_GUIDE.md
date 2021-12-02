# Adding new data to the graph
​
## Summary of the main steps
​
* Preliminary exploration of the new data (see below)
* Create source and processing script (see below)
* Update `db_schema` and `data_integration`  (see below)
* Create `test/source_data/FOLDER` for the new dataset
* Test source script runs OK locally
* Test processing script steps (up until import function)
* Commit to `dev-$USER` branch
​
​
### Preliminary data exploration recommendations
​
Can be done locally in notebooks
​
* Do data checks against the graph, e.g. in jupyter notebook
* Create and test the source script locally (should work without the graph, unless required)
* Manage source and process scripts within the repo codebase (in `workflow/scripts`) and modify ymls (in `config/`) to check that everything syncs
	
### Creating the scripts/ymls
​
* **data_integration.yml**
	* modify the version in `config/`, not `test/config`!
	* `_files_`: this has to match the name of the main output file from the source script
	* `_script_`: this is the name of processing script
	* `_source_`: not used during the build, but should be descriptive, as it will be stored in the `node/rel` property
	* the `node/relationship` name (top line) will be used as the input parameter to run the processing script
		
* **db_schema.yml**
	* modify the version in `config/`, not `test/config`!
	* specify which nodes to use (when adding a rel)
	* specify any other properties that processing script retains in df to be imported in the graph
	
* **Source script**
	* in `workflow/scripts/source/`
	* Save data in raw format from the source in `workflow/source_data/FOLDER` (as a backup)
	* Script should download the latest version of the dataset from source (if possible) 
	* Script should do all filtering/mapping and output a file that will be read by the processing script
​
* **Processing script**
	* in `workflow/scripts/processing/`
	* Read in the main file created by the source script
	* The script may need to rename/subset columns to those which are defined in db_schema for this dataset (i.e. what graph is expecting to import)
	​
​
## Building the graph on server
​
There are 3 steps:
​
1. build the graph as is (optional; to eliminate any issues prior to adding new data)
2. add data (using scripts + ymls)
3. rebuild the graph with new data
​
​
#### Prep
* The graph build has to happen on jojo (or other server)
* (`source ~/.bashrc`) and `conda activate neo4j_build`
* Make a folder in `workflow/source_data/FOLDER`
* Modify `DATA_DIR` in `.env` when running the source script to point at the local source_data folder
​
#### Step 1
​
```
snakemake -r clean_all -j 1
snakemake -r all -j 4
```
If there are issues with missing data, you will need to run scripts that make those datasets manually, to determine the issue (e.g. new data version has extra columns) and fix it to produce the missing data.
​
#### Step 2
​
Assuming scripts and ymls are created and locally tested, run: 
​
```
# run source script
python -m workflow.scripts.source.SOURCE_SCRIPT
​
# run processing script (-d is optional if DATA_DIR in .env is set to a local path)
python -m workflow.scripts.processing.rels.PROCESSING_SCRIPT -n (name in data_integration.yml) -d workflow/source_data/
​
# check new data (gives a short uninformative message as if nothing happened)
snakemake -r check_new_data -j 10
```
​
#### Step 3 
​
```
snakemake -r all -j 4
```
	