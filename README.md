# Sparkwiki - processing tools for wikipedia data

[![Build Status](https://travis-ci.com/epfl-lts2/sparkwiki.svg?branch=master)](https://travis-ci.com/epfl-lts2/sparkwiki)

## Basics
Sparkwiki is a set of tools written in Scala that aims at processing data from 
wikipedia, namely 
* SQL table dumps 
** [info](https://meta.wikimedia.org/wiki/Data_dumps)
** [example download](https://dumps.wikimedia.org/enwiki/)
** [table structure](https://www.mediawiki.org/wiki/Manual:Database_layout)
* pageviews
** [info](https://dumps.wikimedia.org/other/pagecounts-ez/)
** [data download](https://dumps.wikimedia.org/other/pagecounts-ez/merged/)

## Usage

You need:
* an installation of [Apache Spark](https://spark.apache.org/)
* data dumps downloaded from the links mentioned above.

*Important*: for performance reason, you should convert .sql.gz table dumps 
to .sql.bz2 (s.t. Spark can process them in parallel and take advantage of all 
processors in the system)