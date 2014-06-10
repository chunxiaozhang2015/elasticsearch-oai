#!/bin/sh

java="/usr/bin/java"
#java="/Library/Java/JavaVirtualMachines/jdk1.8.0.jdk/Contents/Home/bin/java"
#java="/usr/java/jdk1.8.0/bin/java"

echo '
{
    "elasticsearch" : "es://localhost:9300?es.cluster.name=elasticsearch",
    "client" : "ingest",
    "maxbulkactions" : 100,
    "maxconcurrentbulkrequests" : 10,
    "index" : "europeana1914-1918",
    "type" : "oai_edm",
    "oai" : {
        "url" : "http://europeana1914-1918.eu/oai?verb=ListRecords&metadataPrefix=oai_edm&from=2010-01-01T00:00:00Z&until=2015-01-01T00:00:00Z",
        "handler" : "rdf",
        "scrubxml" : false,
        "trace" : false,
        "deref_index" : "europeana1914-1918",
        "deref_type" : "oai_edm",
        "deref_field" : ["skos:prefLabel","location"],
        "deref_prefix" : "europeana19141918:"
    }
}
' | ${java} \
    -cp $(pwd):$(pwd)/\*:$(pwd)/../../lib/\* \
    org.xbib.elasticsearch.plugin.feeder.Runner \
    org.xbib.elasticsearch.plugin.feeder.oai.OAIFeeder
