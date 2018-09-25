# Sparkwiki - processing tools for wikipedia data

[![Build Status](https://travis-ci.com/epfl-lts2/sparkwiki.svg?branch=master)](https://travis-ci.com/epfl-lts2/sparkwiki)

## Basics
Sparkwiki is a set of tools written in Scala (2.11) that aims at processing data from 
wikipedia, namely 
* SQL table dumps 
  - [info](https://meta.wikimedia.org/wiki/Data_dumps)
  - [example download](https://dumps.wikimedia.org/enwiki/)
  - [table structure](https://www.mediawiki.org/wiki/Manual:Database_layout)
* pageviews
  - [info](https://dumps.wikimedia.org/other/pagecounts-ez/)
  - [data download](https://dumps.wikimedia.org/other/pagecounts-ez/merged/)

## Usage

### Pre-requisites
You need:
* an installation of [Apache Spark](https://spark.apache.org/)
* data dumps downloaded from the links mentioned above.

*Important*: for performance reason, you should convert `.sql.gz` table dumps 
to `.sql.bz2` (s.t. Spark can process them in parallel and take advantage of all 
processors in the system), e.g. unsing a command such as  
`zcat dump.sql.gz | bzip2 > dump.sql.gz`

Every tool can be run via `spark-submit`, e.g.
```
./spark-submit --class ch.epfl.lts2.wikipedia.[ToolNameHere]  --master 'local[*]' --executor-memory 4g --driver-memory 4g --packages org.rogach:scallop_2.11:3.1.3 ./sparkwiki/target/scala-2.11/sparkwiki_2.11-0.5.0.jar [ToolArgsHere]
```

### Dump processor
This tool can be run using the class `ch.epfl.lts2.wikipedia.DumpProcessor` as entry point. It will read the 
SQL table dumps starting with value supplient by the `namePrefix` argument, in the directory specified by the `dumpPath` argument, 
and write (compressed) csv files in the directory specified by `outputPath` that can later be imported in the [neo4j graph database](https://neo4j.com/).

This tool requires the following table dumps to be present:
* page
* pagelinks
* redirect
* categorylinks

**Arguments**:
* `--dumpPath` directory containing **sql.bz2** files (gz not supported)
* `--outputPath` output directory
* `--namePrefix` leading part of each SQL dump, e.g. `enwiki-20180801`


### Dump parser
This tool is available using the class `ch.epfl.lts2.wikipedia.DumpParser` as entry point. It reads a single SQL dump
and converts it to either a csv file, or a parquet file.

**Arguments**:
* `--dumpFilePath` path to the .sql.gz or sql.bz2 SQL dump to read
* `--dumpType` should be *page*, *redirect*, *pagelinks*, *category* or *categorylinks*
* `--outputPath` output directory
* `--outputFormat` (default=*csv*), should be *csv* or *parquet*


### Pagecount processor
This tool can be run using the class `ch.epfl.lts2.wikipedia.PagecountProcessor` as entry point. It will read a collection of *pagecounts* covering the period
between arguments `startDate` and `endDate` (inclusive bounds), 
filter the counts belonging to *en.z* (english wikipedia project) and having more daily visits than a threshold given by the `minDailyVisit` argument
and save the result to a csv, after resolving page ids if either a SQL page dump or a processed SQL page dump (as parquet) is supplied via the `pageDump` argument.

**Arguments**:
* `--basePath` directory containing pagecounts files
* `--outputPath` output directory
* `--startDate` first day to process, formatted as `yyyy-MM-dd`, e.g. 2018-08-03
* `--endDate` last day to process,  formatted as `yyyy-MM-dd`
* `--pageDump` path to a page SQL dump or a version processed by *DumpParser* and saved as parquet
* `--minDailyVisits` minimum number of daily visit for a page to be considered
