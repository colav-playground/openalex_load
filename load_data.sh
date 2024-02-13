#!/bin/bash
## Correr este script parado en data
## primero descomprimir los datos
entity=('authors'  'concepts'  'funders'  'institutions'  'merged_ids'  'publishers'  'sources'  'works')

for ent in "${entity[@]}";
do 
    for i in $(ls $ent/*/*)
    do
        mongoimport -d openalex_new -c $ent --type json --file  $i
        rm -rf $i
    done

done
