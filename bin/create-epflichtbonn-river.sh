curl -XPUT 'localhost:9200/_river/uni_bonn/_meta' -d '{
    "type" : "oai",
    "oai" : {
        "set" : "ulbbnpc",
        "url" : "http://epflicht.ulb.uni-bonn.de/oai/",
        "metadataPrefix" : "oai_dc",
        "from" : "2010-01-15T00:00:00Z",
        "until" : "2013-01-15T00:00:00Z"
    }
}'
