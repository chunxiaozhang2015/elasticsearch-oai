#!/bin/sh

curl -XPUT 'localhost:9200/_river/my_europeana_river/_meta' -d '
{
  "type" : "oai",
  "oai" : {
    "input" : [
        "http://europeana1914-1918.eu/oai?verb=ListRecords&metadataPrefix=oai_edm&from=2010-01-01T00:00:00Z&until=2015-01-01T00:00:00Z"
    ],
    "handler" : "rdf",
    "index" : "europeana1914-1918",
    "type" : "oai_edm",
    "maxbulkactions" : 1000,
    "maxconcurrentbulkrequests" : 10,
    "scrubxml" : false,
    "trace" : false,
    "deref_index" : "europeana1914-1918",
    "deref_type" : "oai_edm",
    "deref_field" : ["skos:prefLabel","location"],
    "deref_prefix" : "europeana19141918:"
  }
}
'