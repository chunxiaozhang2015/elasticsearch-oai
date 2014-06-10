#!/bin/sh

#!/bin/sh

curl -XPUT 'localhost:9200/_river/my_melt_river/_meta' -d '
{
  "type" : "oai",
  "oai" : {
    "input" : [
        "http://www.melt.fwu.de/oai2.php?verb=ListRecords&metadataPrefix=oai_lom",
    ],
    "handler" : "xml",
    "index" : "melt",
    "type" : "oai_lom",
    "maxbulkactions" : 1000,
    "maxconcurrentbulkrequests" : 10,
    "scrubxml" : false,
    "trace" : false,
    "elasticsearch" : "es://localhost:9300?es.cluster.name=elasticsearch"
}
'