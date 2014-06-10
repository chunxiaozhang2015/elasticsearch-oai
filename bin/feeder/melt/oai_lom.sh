#!/bin/sh

java="/usr/bin/java"
#java="/Library/Java/JavaVirtualMachines/jdk1.8.0.jdk/Contents/Home/bin/java"
#java="/usr/java/jdk1.8.0/bin/java"

echo '
{
    "elasticsearch" : "es://localhost:9300?es.cluster.name=elasticsearch",
    "client" : "ingest",
    "index" : "melt",
    "type" : "oai_lom",
    "maxbulkactions" : 1000,
    "maxconcurrentbulkrequests" : 10,
    "oai" : [
        {
            "url" : "http://www.melt.fwu.de/oai2.php?verb=ListRecords&metadataPrefix=oai_lom",
            "handler" : "xml",
            "scrubxml" : false,
            "trace" : false
        }
    ]
}
' | ${java} \
    -cp $(pwd):$(pwd)/\*:$(pwd)/../../lib/\* \
    org.xbib.elasticsearch.plugin.feeder.Runner \
    org.xbib.elasticsearch.plugin.feeder.oai.OAIFeeder
