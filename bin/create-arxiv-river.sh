
curl -XPUT 'localhost:9200/_river/my_arxiv_river/_meta' -d '
{
    "type" : "oai",
    "oai" : {
        "url" : "http://export.arxiv.org/oai2",
        "set" : "cs"        
    },
    "index" : {
        "index" : "arxiv",
        "type"  : "cs"
    }    
}
'
