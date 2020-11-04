### Author:-----------------Valeriia Haberland----------------------###
### Script: Cleaning of the IntAct tab-delimited files (intact.zip) ###
#######################################################################

#Removing all non-human records
awk -F "\t" '{if(($10 ~ /human/) && ($11 ~ /human/)) print $0}' intact.txt > human_intact.txt

#Removing all records where the interacting molecules are not proteins or genes
awk -F "\t" '{if(($21 ~ /gene|protein/) && ($22 ~ /gene|protein/)) print $0}' human_intact.txt > human_intact_proteins.txt

#Removing all records where the interacting molecules have no uniprot or ensembl IDs
#awk -F "\t" '{print $22}' human_intact_proteins.txt | sort | uniq
awk -F "\t" '{if(($1 ~ /uniprotkb|ensembl/) && ($2 ~ /uniprotkb|ensembl/)) print $0}' human_intact_proteins.txt > human_intact_proteins_ids.txt
wc -l human_intact_proteins_ids.txt

#Choosing only the columns which identify the interating molecules, interation and publication
awk -F "\t" 'BEGIN{OFS = "\t"}{print $1,$2,$7,$8,$9,$12,$13,$14,$15,$21,$22}' human_intact_proteins_ids.txt > human_intact_proteins_ids_clms.txt

#Add info related to whether these are negative interations
gawk -F "\t" 'BEGIN{OFS = "\t"}{print $0,"false"}' human_intact_proteins_ids_clms.txt > temp.txt
cp temp.txt human_intact_proteins_ids_clms.txt

#Processing the dataset with the negative interactions
awk -F "\t" '{if(($10 ~ /human/) && (($11 ~ /human/)) print $0}' intact_negative.txt > human_intact_negative.txt
awk -F "\t" '{if(($21 ~ /gene|protein/) && ($22 ~ /gene|protein/)) print $0}' human_intact_negative.txt > human_intact_negative_proteins.txt
awk -F "\t" '{if(($1 ~ /uniprotkb|ensembl/) && ($2 ~ /uniprotkb|ensembl/)) print $0}' human_intact_negative_proteins.txt > human_intact_proteins_negative_ids.txt
awk -F "\t" 'BEGIN{OFS = "\t"}{print $1,$2,$7,$8,$9,$12,$13,$14,$15,$21,$22}' human_intact_proteins_negative_ids.txt > human_intact_proteins_negative_ids_clms.txt
gawk -F "\t" 'BEGIN{OFS = "\t"}{print $0,"true"}' human_intact_proteins_negative_ids_clms.txt > temp.txt
cp temp.txt human_intact_proteins_negative_ids_clms.txt

#Merging positive and negative interactions
cat human_intact_proteins_ids_clms.txt human_intact_proteins_negative_ids_clms.txt > human_intact_all.txt

#Removing brackets, unnecessary identifiers, etc.
sed "s/uniprotkb://g" human_intact_all.txt > temp.txt
cp temp.txt human_intact_all.txt
sed "s/ensembl://g" human_intact_all.txt > temp.txt
cp temp.txt human_intact_all.txt
sed "s/\"psi-mi:\"\"MI:[0-9]*\"//g" human_intact_all.txt > temp.txt
cp temp.txt human_intact_all.txt
sed "s/(//g" human_intact_all.txt > temp.txt
cp temp.txt human_intact_all.txt
sed "s/)//g" human_intact_all.txt > temp.txt
cp temp.txt human_intact_all.txt

#Removing all other scores, except intact-miscore
awk -F "\t" '{print $9}' human_intact_all.txt > temp.txt
awk '{print $NF}' FS="|" temp.txt >tempp.txt
sed "s/\"//g" tempp.txt > temp.txt
paste human_intact_all.txt temp.txt  | column -s $'\t' -t > tempp.txt
awk -F "\t" 'BEGIN{OFS = "\t"}{print $1,$2,$3,$4,$5,$6,$7,$8,$10,$11,$12,$13}' tempp.txt >temp.txt
cp temp.txt human_intact_all.txt
sed 's/intact-miscore://g' human_intact_all.txt > temp.txt
cp temp.txt human_intact_all.txt

#Cleaning interaction IDs (only intact are used)
awk -F "\t" 'BEGIN{OFS = "\t"}{gsub("\\|.*","\"",$8);print $0}' human_intact_all.txt > temp.txt
cp temp.txt human_intact_all.txt

#Removing all publication IDs, except pubmed (no universal regex; cleaned manually to the large extent!!!)
sed 's/mint\:MINT-[0-9]*\|//g' human_intact_all.txt >temp.txt
#...
#Remove .r1 and .r2
awk -F "\t" 'BEGIN{OFS = "\t"}{gsub("\\.r1|\\.r2","",$12);print $0}' human_intact_all.txt > temp.txt
mv temp.txt human_intact_all.txt

#Merging the isoforms
#Note!!! A confidence score for the isoforms is aggregated by choosing the maximum value.
awk -F "\t" 'BEGIN{OFS = "\t"}{print $0,$1,$2}' human_intact_all.txt > temp.txt # append the original names
#Remove the isoform identifiers 
awk -F "\t" 'BEGIN{OFS = "\t"}{gsub("\\-[0-9a-zA-Z_]*","",$1);gsub("\\-[0-9a-zA-Z_]*","",$2);print $0}' human_intact_all.txt > human_intact_all_merged_isoforms.txt 
#Remove the '.'
awk -F "\t" 'BEGIN{OFS = "\t"}{gsub("\\.","",$1);gsub("\\.","",$2);gsub("\\.","",$13);gsub("\\.","",$14);print $0}' human_intact_all_merged_isoforms.txt > temp.txt 
mv temp.txt human_intact_all_merged_isoforms.txt 

#Choose only the unique rows
awk -F "\t" 'BEGIN{OFS = "\t"}{print $0}' human_intact_all_merged_isoforms.txt | sort | uniq > temp.txt
mv temp.txt human_intact_all_merged_isoforms.txt

#####################################Creating files for upload###########################################
#Create "relationships" files for neo4j upload
awk -F "\t" '{if(($8 ~ /protein/) && ($9 ~ /protein/) && ($10 ~ /false/)) print $0}' human_intact_all_merged_isoforms.txt >human_intact_proteins_positive.txt
awk -F "\t" '{if(($8 ~ /protein/) && ($9 ~ /protein/) && ($10 ~ /true/)) print $0}' human_intact_all_merged_isoforms.txt >human_intact_proteins_negative.txt
awk -F "\t" 'BEGIN{OFS = "\t"}{if((($8 ~ /gene/) && ($9 ~ /protein/) && ($10 ~ /false/)) || (($8 ~ /protein/) && ($9 ~ /gene/) && ($10 ~ /false/))) print $0}' human_intact_all_merged_isoforms.txt >human_intact_proteins_genes_positive.txt
awk -F "\t" 'BEGIN{OFS = "\t"}{if(($8 ~ /gene/) && ($9 ~ /gene/) && ($10 ~ /false/)) print $0}' human_intact_all_merged_isoforms.txt >human_intact_genes_positive.txt

#Merge the headers
cat header_short.txt human_intact_proteins_positive.txt >temp.txt
mv temp.txt human_intact_proteins_positive.txt
cat header_short.txt human_intact_proteins_negative.txt >temp.txt
mv temp.txt human_intact_proteins_negative.txt 
cat header_short.txt human_intact_proteins_genes_positive.txt >temp.txt
mv temp.txt human_intact_proteins_genes_positive.txt 
cat header_short.txt human_intact_genes_positive.txt >temp.txt
mv temp.txt human_intact_genes_positive.txt

#Create the publications unique list for the neo4j upload; removed "ID_interactor_" from the rows manually,etc.
awk -F "\t" 'BEGIN{OFS = "\t"}{if($12 !~ /unassigned/) print $1,$12}' human_intact_all_merged_isoforms.txt | sort | uniq > temp.txt
awk -F "\t" 'BEGIN{OFS = "\t"}{if($12 !~ /unassigned/) print $2,$12}' human_intact_all_merged_isoforms.txt | sort | uniq > tempp.txt
cat temp.txt tempp.txt | sort | uniq > human_intact_publications.txt 
awk -F "\t" '{if($1 ~ /ENSG|ENST/) print $0}' human_intact_publications.txt >human_intact_publications_genes.txt
awk -F "\t" '{if($1 !~ /ENSG|ENST/) print $0}' human_intact_publications.txt >human_intact_publications_proteins.txt

#Add headers
cat header_publications.txt human_intact_publications_genes.txt > temp.txt
mv temp.txt human_intact_publications_genes.txt
cat header_publications.txt human_intact_publications_proteins.txt > temp.txt
mv temp.txt human_intact_publications_proteins.txt

#Create the unique lists of proteins and genes
#Note!!! Removed manually "Protein_gene" row from each file, added a one-column header "Proteins" and "Genes" respectively;
#removed "ID_interactor_A" or "B" from the rows manually
awk -F "\t" '{print $1}' human_intact_publications_proteins.txt | sort| uniq > all_proteins.txt
awk -F "\t" '{print $1}' human_intact_publications_genes.txt | sort| uniq > all_genes.txt 

