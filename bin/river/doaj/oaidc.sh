#!/bin/sh

curl -XPUT 'localhost:9200/_river/my_doaj_river/_meta' -d '
{
  "type" : "oai",
  "oai" : {
    "input" : [
        "http://doaj.org/oai?verb=ListRecords&metadataPrefix=oai_dc&from=2000-01-01&until=2014-04-17"
    ],
    "handler" : "xml",
    "index" : "doaj",
    "type" : "oai_dc",
    "maxbulkactions" : 1000,
    "maxconcurrentbulkrequests" : 20,
    "trace" : false,
    "scrubxml" : false
}
'