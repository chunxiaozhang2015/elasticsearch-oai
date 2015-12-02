![OAI-PMH](https://github.com/jprante/elasticsearch-oai/raw/master/src/site/resources/OA200.gif)

Image Copyright (C) [OAI](http://www.openarchives.org/)

# OAI Harvester for Elasticsearch [![Travis](https://travis-ci.org/jprante/elasticsearch-oai.png)](https://travis-ci.org/jprante/elasticsearch-oai)

The [Open Archives Initiative - Protocol for Metadata Harvesting (OAI-PMH)](http://www.openarchives.org/pmh/)
client allows to harvest metadata into Elasticsearch.

This application for Elasticsearch can run as a feeder in a standalone JVM, and connects to a remote cluster.

It harvests DC / XML / RDF formats from OAI data providers (OAI servers).

A list of OAI servers can be found [here](http://www.openarchives.org/Register/BrowseSites)
The metadata is internally represented as resources, using Resource Description Framework (RDF) of
the W3C Semantic Web Initiative, before the resources are serialized into JSON for
Elasticsearch schema-less indexing.

## Versions

| Elasticsearch version    | Plugin     | Release date |
| ------------------------ | -----------| -------------|
| 2.1.0                    | 2.1.0.0    | Dec  2, 2015 |
| 1.5.1                    | 1.5.1.0    | Apr 22, 2015 |
| 1.2.1                    | 1.2.1.0    | Jun 10, 2014 |
| 1.1.0                    | 1.1.0.0    | Apr 24, 2014 |
| 1.1.0                    | 1.1.0.1    | May 10, 2014 |

## Installation

    mkdir -p lib bin
    cd lib
    curl -O 'xbib.org/repository/org/xbib/elasticsearch/plugin/elasticsearch-oai/2.1.0.0/elasticsearch-oai-2.1.0.0-standalone.jar'
    cd ..
    <create a bash feed.sh script in bin folder>
    <add log4j2.xml to bin folder>
    ./bin/feed.sh

## Project docs

The Maven project site is available at
[Github](http://jprante.github.io/elasticsearch-oai)

## Issues

All feedback is welcome! If you find issues, please post them at
[Github](https://github.com/jprante/elasticsearch-oai/issues)

# Documentation

A feeder is a standalone application that can push data into a remote Elasticsearch cluster and
runs outside an Elasticsearch node. This push mode is similar to Logstash, which is a
data pipeline tool that can prepare event-based data for Elasticsearch.

You can create a feeder script for example for indexing the Arxiv repository:

    ./bin/arxiv.sh

where the shell script has the content::


    #!/bin/bash

    # cron?
    tty -s
    if [ "$?" -gt "0" ]
    then
        # assume this working dir in $HOME
        cd $HOME/oai-tool
        pwd=$(pwd)
        bin=${pwd}/bin
        lib=${pwd}/lib
    else
        pwd="$( cd -P "$( dirname "$0" )" && pwd )"
        bin=${pwd}/../bin
        lib=${pwd}/../lib
    fi

    java="java"

    # arxiv.org is throttling to 20sec by HTTP Status 503 retry-after.
    # concurrency should be 1.

    echo '
    {
        "uri" : [
            "http://export.arxiv.org/oai2?verb=ListRecords&metadataPrefix=arXiv&from=2000-01-01&until=2016-01-01"
        ],
        "concurrency" : 1,
        "elasticsearch" : {
            "cluster" : "elasticsearch",
            "host" : "localhost",
            "port" : 9300
        },
        "index" : "arxiv",
        "type" : "arxiv",
        "maxbulkactions" : 1000,
        "maxconcurrentbulkrequests" : 1,
        "mock" : true,
        "timewindow" : "yyyyMMddHH",
        "aliases" : true,
        "ignoreindexcreationerror" : true

    }
    ' | ${java} \
        -cp ${lib}/\*:${bin}/\* \
        -Dlog4j.configurationFile=${bin}/log4j2.xml \
        org.xbib.tools.Runner \
        org.xbib.tools.OAIFeeder


Before running, please check where your Java 8 installation is located, and fix the ``java`` variable setting.

More examples can be found in the `bin` folder of the repository.

## Logging

The logging can be controlled by the `log4j2.xml` file in the bin folder.

## Parameters

`uri` - a list of URLs for harvesting

`concurrency` - how many URLs should be processed simultaneously

`elasticsearch` - connection data for a node in an Elasticsearch cluster

`index` - the name of the Elasticsearch index

`type` - the name of the Elasticsearch index type

`maxbulkactions` - the maximum number of actions in a bulk request

`maxconcurrentbulkrequests` - the maximum number of concurrent bulk requests

`mock` - set to `true` if harvested docs should not be indexed but logged instead

`timewindow` - appendix for index name if date-stamp is wanted, like `yyyyMMddHH`

`aliases` - set index aliases automatically

 `ignoreindexcreationerror` - if true, do not fail with error when index already exists

## License

Elasticsearch OAI Harvester

Copyright (C) 2014 Jörg Prante

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.