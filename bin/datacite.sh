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

echo '
{
    "uri" : [
        "http://oai.datacite.org/oai?verb=ListRecords&metadataPrefix=oai_dc&set=CDL.DPLANET"
    ],
    "concurrency" : 1,
    "elasticsearch" : {
        "cluster" : "elasticsearch",
        "host" : "localhost",
        "port" : 9300
    },
    "index" : "datacite",
    "type" : "datacite",
    "maxbulkactions" : 1000,
    "maxconcurrentbulkrequests" : 1,
    "mock" : false,
    "timewindow" : "yyyyMMddHH",
    "aliases" : true,
    "ignoreindexcreationerror" : true

}
' | ${java} \
    -cp "${lib}/*" \
    -Dlog4j.configurationFile=${bin}/log4j2.xml \
    org.xbib.tools.Runner \
    org.xbib.tools.OAIFeeder
