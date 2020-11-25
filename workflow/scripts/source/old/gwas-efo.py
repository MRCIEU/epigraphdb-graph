import gzip
import json
from SPARQLWrapper import SPARQLWrapper, JSON

#### PROBLEM
#Unfortunately the RDF Platform has no funding currently and as such support of this service is sporadic at best. 
# If you are depending on the RDF Platform in your organization, or you are willing to consider funding the RDF platform, 
# please contact us at rdf-req@ebi.ac.uk with details regarding your project.
###


def get_efo():
    efo_json='efo.json'
    # https://io.datascience-paris-saclay.fr/exampleView.php?iri=https://io.datascience-paris-saclay.fr/query/EFO_Query
    sparql = SPARQLWrapper("https://www.ebi.ac.uk/rdf/services/sparql")
    sparql.setQuery(
        """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?child ?childLabel ?parent ?parentLabel
    FROM <http://rdf.ebi.ac.uk/dataset/efo>
        WHERE {
        ?child rdfs:subClassOf ?parent .
        ?child rdfs:label ?childLabel .
        ?parent rdfs:label ?parentLabel .
        }
    """
    )
    print(sparql)
    # LIMIT 100""")
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    print(results)
    data = results["results"]["bindings"]
    print(data)
    with gzip.open(efo_json, "wt", encoding="ascii") as outfile:
        json.dump(data, outfile)


# embed and index all EFO terms
# embed each GWAS trait and create cosine distance
# notebooks/shared-rw/vectology/clinical-trials.ipynb
#def nlp_efo():
    #todo

if __name__ == "__main__":
    get_efo()