#!/bin/sh

# Mac OS X
java="/Library/Java/JavaVirtualMachines/jdk1.8.0.jdk/Contents/Home/bin/java"

# Linux
#java="/usr/java/jdk1.8.0/bin/java"

# arxiv.org is throttling to 20 sec by HTTP STatus 503 retry-after.
# concurrency should be 1.

echo '
{
    "input" : [
        "http://export.arxiv.org/oai2?verb=ListRecords&metadataPrefix=arXiv&from=2000-01-01&until=2015-01-01"
    ],
    "concurrency" : 1,
    "handler" : "xml",
    "elasticsearch" : "es://localhost:9300?es.cluster.name=elasticsearch",
    "index" : "arxiv",
    "type" : "arxiv",
    "shards" : 1,
    "replica" : 0,
    "maxbulkactions" : 1000,
    "maxconcurrentbulkrequests" : 1,
    "client" : "bulk",
    "trace" : false,
    "scrubxml" : false
}
' | ${java} \
    -cp $(pwd):$(pwd)/\*:$(pwd)/../../lib/\* \
    org.xbib.elasticsearch.plugin.feeder.Runner \
    org.xbib.elasticsearch.plugin.feeder.oai.OAIFeeder
