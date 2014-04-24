#!/bin/sh

java="/Library/Java/JavaVirtualMachines/jdk1.8.0.jdk/Contents/Home/bin/java"
#java="/usr/java/jdk1.8.0/bin/java"

echo '
{
    "input" : [
        "http://doaj.org/oai?verb=ListRecords&metadataPrefix=oai_dc&from=2000-01-01&until=2014-04-17"
    ],
    "handler" : "xml",
    "elasticsearch" : "es://localhost:9300?es.cluster.name=elasticsearch",
    "index" : "doaj",
    "type" : "oai_dc",
    "maxbulkactions" : 1000,
    "maxconcurrentbulkrequests" : 20,
    "client" : "bulk",
    "trace" : false,
    "scrubxml" : false
}
' | ${java} \
    -cp $(pwd):$(pwd)/\*:$(pwd)/../../lib/\* \
    org.xbib.elasticsearch.plugin.feeder.Runner \
    org.xbib.elasticsearch.plugin.feeder.oai.OAIFeeder
