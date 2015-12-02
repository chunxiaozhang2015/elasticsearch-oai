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
    "mock" : false,
    "timewindow" : "yyyyMMddHH",
    "aliases" : true,
    "ignoreindexcreationerror" : true
}
' | ${java} \
    -cp ${lib}/\* \
    -Dlog4j.configurationFile=${bin}/log4j2.xml \
    org.xbib.tools.Runner \
    org.xbib.tools.OAIFeeder
