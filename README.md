# SparkWiki - processing tools for Wikipedia data

![Build Status](https://github.com/epfl-lts2/sparkwiki/actions/workflows/build.yml/badge.svg)

### Use cases
SparkWiki toolkit can be used in various scenarios where you are interested in researching Wikipedia graph and pageview statistics. Graph and pageviews can be used and studied separately. You can see a few examples below.

**Detecting Spatio-temporal Anomalies in Wikipedia Viewership Statistics**

This toolkit was used in the large-scale experiments for an algorithm performing [Anomaly Detection in Dynamic Graphs](https://arxiv.org/abs/1901.09688). You can see an example of the usage of the pre-processed data in [`ch.epfl.lts2.wikipedia.PagecountProcessor`](https://github.com/epfl-lts2/sparkwiki/blob/master/src/main/scala/ch/epfl/lts2/wikipedia/PeakFinder.scala). This class contains an implementation of the algorithm. The experiments used Wikipedia graph and pagecounts to detect anomalies in viewership statistics of Wikipedia visitors. The graph and pagecounts data are pre-processed using the tools presented in this repository. You can find a brief demo [here](https://github.com/mizvol/anomaly-detection).

**What is Trending on Wikipedia?**

The same [algorithm and implementation](https://github.com/epfl-lts2/sparkwiki/blob/master/src/main/scala/ch/epfl/lts2/wikipedia/PeakFinder.scala) were used to detect trends in multiple language editions of Wikipedia. See more details in the [paper](https://arxiv.org/abs/2002.06885) and a [short 8-min presentation](https://www.youtube.com/watch?v=Oa6WPOv6sHQ).

**Forecasting Wikipedia Page Views with Graph Embeddings**

Another project used this toolkit to pre-process pagecounts. The project's goal was to forecast page-views on Wikipedia. See more details [here](http://cs229.stanford.edu/proj2019aut/data/assignment_308832_raw/26647399.pdf).

**A Knowledge-graph based Taxonomy Construction Method**

A knowledge-graph project used SparkWiki toolkit to construct a Wikipedia-based knowledge graph. See more details [here](http://www.inf.u-szeged.hu/~london/publ/AutoTaxLisboaPoster.pdf)

## Basics
Sparkwiki is a set of tools written in Scala (2.12) that aims at processing data from 
Wikipedia, namely 
* SQL table dumps 
  - [info](https://meta.wikimedia.org/wiki/Data_dumps)
  - [example download](https://dumps.wikimedia.org/enwiki/)
  - [table structure](https://www.mediawiki.org/wiki/Manual:Database_layout)
* pageviews
  - [info](https://dumps.wikimedia.org/other/pageview_complete/readme.html)
  - [data download](https://dumps.wikimedia.org/other/pageview_complete/)

## Usage

There is a detailed deployment tutorial available [here](https://github.com/epfl-lts2/sparkwiki/tree/master/helpers). Consise instructions are provided below.

### Pre-requisites
You need:
* an installation of [Apache Spark](https://spark.apache.org/)
* [Scala sbt](https://www.scala-sbt.org/)
* data dumps downloaded from the links mentioned above.

*Important*: for performance reason, you should convert `.sql.gz` table dumps 
to `.sql.bz2` (s.t. Spark can process them in parallel and take advantage of all 
processors in the system), e.g. unsing a command such as  
`zcat dump.sql.gz | bzip2 > dump.sql.gz`

Every tool can be run via `spark-submit`, e.g.
```
./spark-submit --class ch.epfl.lts2.wikipedia.[ToolNameHere]  --master 'local[*]' --executor-memory 4g --driver-memory 4g --packages org.rogach:scallop_2.12:4.1.0 ./sparkwiki/target/scala-2.12/sparkwiki_2.12-1.0.0.jar [ToolArgsHere]
```

### Build
From the project directory, run `sbt package` to build the jar file. If you want to edit the code, run `sbt eclipse`

### Dump processor
This tool can be run using the class `ch.epfl.lts2.wikipedia.DumpProcessor` as entry point. It will read the 
SQL table dumps starting with value supplied by the `namePrefix` argument, in the directory specified by the `dumpPath` argument, 
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
This tool is available using the class `ch.epfl.lts2.wikipedia.DumpParser` as entry point. It reads one or more SQL dumps
and converts them to either a csv file, or a parquet file. All the dumps should be of the same type (e.g. all `pages`)
but can span multiple languages. Language will be extracted from the dump filename and appended to each record in the output.

**Arguments**:
* `--dumpFilePaths` path to the .sql.gz or sql.bz2 SQL dumps to read
* `--dumpType` should be *page*, *redirect*, *pagelinks*, *category*, *categorylinks* or *langlinks*
* `--outputPath` output directory
* `--outputFormat` (default=*csv*), should be *csv* or *parquet*
* `--languages` (optional, only used for *langlinks*), extract only links pointing to the specified languages, 
e.g. `--languages fr es` to keep only links pointing to french and spanish versions.


### Pagecount processor
This tool can be run using the class `ch.epfl.lts2.wikipedia.PagecountProcessor` as entry point. It will read a collection of *pagecounts* covering the period
between arguments `startDate` and `endDate` (inclusive bounds), 
filter the counts belonging to wikipedia project and having more daily visits than a threshold given by the `minDailyVisit` argument and save the result to Parquet files or into a Cassandra database, after resolving page ids (either a SQL page dump or a processed SQL page dump (as parquet) must be supplied via the `pageDump` argument).

**Arguments**:
* `--config` path to configuration file (cf. *config* folder for a sample)
* `--basePath` directory containing pagecounts files
* `--startDate` first day to process, formatted as `yyyy-MM-dd`, e.g. 2018-08-03
* `--endDate` last day to process,  formatted as `yyyy-MM-dd`
* `--languages` list of languages to extract pagecounts for, e.g. `--languages en fr ru` to process french, english and russian languages. 
* `--pageDump` path to a page SQL dump or a version processed by *DumpParser* and saved as parquet
* `--outputPath` path to Parquet files with pre-processed pagecounts. You can use these Parquet files for further processing in Spark or any other framework that supports Parquet format. 
If you omit this option, you need a running Cassandra instance to store pagecount data (and set the appropriate connection information in the configuration file).

### License

Please cite [this](https://arxiv.org/abs/1903.08597) paper if you use the code or the pre-processed dataset.

```
@article{aspert2019graph,
  title={A Graph-structured Dataset for Wikipedia Research},
  author={Aspert, Nicolas and Miz, Volodymyr and Ricaud, Benjamin and Vandergheynst, Pierre},
  conference={Companion Proceedings of the 2019 World Wide Web Conference},
  year={2019}
}
```
