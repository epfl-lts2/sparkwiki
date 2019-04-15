#! /bin/sh
target_db="wikipedia.db"
delim="\t"
data_dir=/mnt/data/Datasets/wikipedia/dumps-pre-processed
part_template="part-\d{5}-.*.csv.gz"
neo4j-admin import \
    --database=$target_db --delimiter=$delim \
    --report-file=/tmp/import-wiki.log \
    --id-type=INTEGER \
    --nodes:Page import/page_header.csv,"$data_dir/page/normal_pages/$part_template" \
    --nodes:Page:Category import/page_header.csv,"$data_dir/page/category_pages/$part_template" \
    --relationships:LINKS_TO import/pagelinks_header.csv,"$data_dir/pagelinks/$part_template" \
    --relationships:BELONGS_TO import/categorylinks_header.csv,"$data_dir/categorylinks/$part_template" \
    --ignore-missing-nodes