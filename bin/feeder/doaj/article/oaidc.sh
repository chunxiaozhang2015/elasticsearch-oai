#!/bin/sh

java="/usr/bin/java"
#java="/Library/Java/JavaVirtualMachines/jdk1.8.0.jdk/Contents/Home/bin/java"
#java="/usr/java/jdk1.8.0/bin/java"

echo '
{
    "elasticsearch" : "es://localhost:9300?es.cluster.name=elasticsearch",
    "client" : "bulk",
    "index" : "doajarticle",
    "type" : "oai_dc",
    "maxbulkactions" : 1000,
    "maxconcurrentbulkrequests" : 10,
    "oai" : {
        "url" : "http://doaj.org/oai.article?verb=ListRecords&metadataPrefix=oai_dc&from=2000-01-01&until=2015-01-01"
        "handler" : "xml",
        "trace" : false,
        "scrubxml" : false
    }
}
' | ${java} \
    -cp $(pwd):$(pwd)/\*:$(pwd)/../../lib/\* \
    org.xbib.elasticsearch.plugin.feeder.Runner \
    org.xbib.elasticsearch.plugin.feeder.oai.OAIFeeder
