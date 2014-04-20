#!/bin/sh
curl -XPUT 'localhost:9200/_river/bonn-epflicht/_meta' -d '{
  "type" : "oai",
  "oai" : {
    "input" : [
        "http://epflicht.ulb.uni-bonn.de/oai/?verb=ListRecords&metadataPrefix=oai_dc&set=ulbbnpc&from=2010-01-01&until=2015-01-01"
    ],
    "concurrency" : 1,
    "handler" : "xml",
    "index" : "bonn-epflicht",
    "type" : "oai_dc",
    "shards" : 1,
    "replica" : 0,
    "maxbulkactions" : 1000,
    "maxconcurrentbulkrequests" : 1,
    "trace" : false,
    "scrubxml" : false
  }

}'
