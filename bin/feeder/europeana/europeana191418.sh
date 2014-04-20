#!/bin/sh

java="/Library/Java/JavaVirtualMachines/jdk1.8.0.jdk/Contents/Home/bin/java"
#java="/usr/java/jdk1.8.0/bin/java"

echo '
{
    "input" : [
        "http://europeana1914-1918.eu/oai?metadataPrefix=oai_edm&from=2000-01-01T00:00:00Z&until=2015-01-01T00:00:00Z"
    ],
    "handler" : "rdf",
    "elasticsearch" : "es://localhost:9300?es.cluster.name=elasticsearch",
    "index" : "europeana1914-1918",
    "type" : "oai",
    "shards" : 3,
    "replica" : 0,
    "maxbulkactions" : 100,
    "maxconcurrentbulkrequests" : 10,
    "client" : "ingest",
    "scrubxml" : true,
    "trace" : true
}
' | ${java} \
    -cp $(pwd):$(pwd)/\*:$(pwd)/../../lib/\* \
    org.xbib.elasticsearch.plugin.feeder.Runner \
    org.xbib.elasticsearch.plugin.feeder.oai.OAIFeeder
