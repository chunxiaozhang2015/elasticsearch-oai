#!/bin/sh

curl -XPUT 'localhost:9200/_river/my_arxiv_river/_meta' -d '
{
  "type" : "oai",
  "oai" : {
    "input" : [
        "http://export.arxiv.org/oai2?verb=ListRecords&metadataPrefix=arXiv&from=2000-01-01&until=2015-01-01"
    ],
    "concurrency" : 1,
    "handler" : "xml",
    "index" : "arxiv",
    "type" : "arxiv",
    "shards" : 1,
    "replica" : 0,
    "maxbulkactions" : 1000,
    "maxconcurrentbulkrequests" : 1,
    "trace" : false,
    "scrubxml" : false
  }
}
'
