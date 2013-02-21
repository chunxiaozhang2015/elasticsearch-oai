OAI River Plugin for ElasticSearch
==================================

The [Open Archives Initiative (OAI)](http://www.openarchives.org/pmh/) [river](http://www.elasticsearch.org/guide/reference/river/) allows to harvest metadata from OAI data providers into ElasticSearch. It is provided as a [plugin](http://www.elasticsearch.org/guide/reference/modules/plugins.html).

The river allows to automatically harvest RDF / DC / XML metadata from OAI data providers (OAI servers). Metadata is internally represented as resources, using Resource Description Framework of the W3C Semantic Web Initiative. These resources are serialized into JSON for Elasticsearch schema-less indexing.

Setting it up is as simple as executing the following against ElasticSearch:

	curl -XPUT 'localhost:9200/_river/my_oai_harvester_1/_meta' -d '{
	    "type" : "oai",
	    "oai" : {
	        "url" : "http://pub.uni-bielefeld.de",
	        "set" : "bi",
	        "metadataPrefix" : "oai_dc"
	    }
	}'

This call will create a river that harvests all the Dublin Core metadata within the data provider `pub.uni-bielefeld.de` within the set `bi`.

In case of a failover, the OAI river will automatically be started on another ElasticSearch node, and continue indexing from the last OAI harvest state.

Many OAI rivers can run in parallel. A list of OAI servers can be found [here] (http://www.openarchives.org/Register/BrowseSites)

Installation
------------

The version is 2.0.0 (21 Feb 2013)

Prerequisites:

- Elasticsearch 0.20.x

- Java 7 Runtime Environment

In order to install the plugin, run: `bin/plugin -url http://bit.ly/Vzerd7 -install elasticsearch-river-oai`.

Bintray site: https://bintray.com/pkg/show/general/jprante/elasticsearch-plugins/elasticsearch-river-oai

Download link: http://dl.bintray.com/content/jprante/elasticsearch-plugins/org/xbib/elasticsearch/elasticsearch-river-oai/2.0.0/elasticsearch-river-oai-2.0.0.zip?direct


Documentation
-------------

The Maven project site is [here](http://jprante.github.com/elasticsearch-river-oai)

The Javadoc API can be found [here](http://jprante.github.com/elasticsearch-river-oai/apidocs/index.html)

Configuration
-------------

Each OAI server describes itself with the verbs `Identify`, `ListSets`, and `ListMetadataFormats`. Harvesting OAI servers is possible by specifying date periods. If a river is configured with a date period, e.g.

	curl -XPUT 'localhost:9200/_river/my_oai_harvester_1/_meta' -d '{
	    "type" : "oai",
	    "oai" : {
	        "url" : "http://pub.uni-bielefeld.de",
	        "set" : "bi",
	        "metadataPrefix" : "oai_dc",
	        "from" : "2012-05-01T00:00:00Z",
	        "until" : "2012-05-02T00:00:00Z"
	    }
	}'

This will harvest the metadata with a server defined date between 2012-05-01T00:00:00Z and 2012-05-02T00:00:00Z. 

Resumption tokens are handled automatically.

If the period is harvested, the date period is increased automatically. Harvesting is throttled when the time period exceeds the current date.

To delay harvesting between time periods, you can give a parameter `poll`. `poll` is a Elasticsearch [TimeValue](https://github.com/elasticsearch/elasticsearch/blob/master/src/main/java/org/elasticsearch/common/unit/TimeValue.java) type. This is convenient if updates are rare.

	curl -XPUT 'localhost:9200/_river/my_oai_harvester_1/_meta' -d '{
	    "type" : "oai",
	    "oai" : {
	        "url" : "http://pub.uni-bielefeld.de",
	        "set" : "bi",
	        "metadataPrefix" : "oai_dc",
	        "from" : "2012-05-01T00:00:00Z",
	        "until" : "2012-05-02T00:00:00Z",
	        "poll" : "5m"
	    }
	}'

Index
-----

Each river can index into a specified index. Example:

	curl -XPUT 'localhost:9200/_river/my_oai_harvester_1/_meta' -d '{
	    "type" : "oai",
	    "oai" : {
	        "url" : "http://pub.uni-bielefeld.de",
	        "set" : "bi",
	        "metadataPrefix" : "oai_dc",
	        "from" : "2012-05-01T00:00:00Z",
	        "until" : "2012-05-02T00:00:00Z",
	        "poll" : "5m"
	    },
	    "index" : {
	        "index" : "metadata",
	        "type" : "oai"
	    }
	}'

Bulk indexing
------------

Bulk indexing is automatically done in order to speed up the indexing process. 
Each OAI response will be indexed by a single bulk. A bulk size can be defined, also a maximum size of active bulk requests to cope with high load situations. A bulk timeout defines the time period after which bulk feeds continue.

	curl -XPUT 'localhost:9200/_river/my_oai_harvester_1/_meta' -d '{
	    "type" : "oai",
	    "oai" : {
	        "url" : "http://pub.uni-bielefeld.de",
	        "set" : "bi",
	        "metadataPrefix" : "oai_dc",
	        "proxyhost" : "proxyserver.domain",
	        "proxyport" : 3128
	    },
	    "index" : {
	        "index" : "metadata",
	        "type" : "oai",
	        "bulk_size" : 100,
	        "max_bulk_requests" : 30
	    }
	}'

HTTP proxy
----------

A HTTP proxy can be defined with `proxyhost` and `proxyport`.

	curl -XPUT 'localhost:9200/_river/my_oai_harvester_1/_meta' -d '{
	    "type" : "oai",
	    "oai" : {
	        "url" : "http://pub.uni-bielefeld.de",
	        "set" : "bi",
	        "metadataPrefix" : "oai_dc",
	        "proxyhost" : "proxyserver.domain",
	        "proxyport" : 3128
	    }
	}'

Error handling
-------------

OAI servers can respond with a variety of error conditions. If an error occurs, the OAI river is stopped. Then, it must be started again by deleting the river and re-adjusting the harvest date period manually.

Stopping the river:

	curl -XDELETE 'localhost:9200/_river/my_dnb_oai_river/'

Starting the river:

	curl -XPUT 'localhost:9200/_river/my_dnb_oairiver/_meta' -d '{
		"type" : "oai", 
		"oai" : { 
			"url" : "http://services.dnb.de/oai/repository", 
			"set" : "authorities", 
			"metadataPrefix" : "RDFxml", 
			"from" : "2012-02-24T00:00:00Z",
			"until" : "2012-02-25T00:00:00Z", 
			"poll" : "15s", 
			"proxyhost" : "localhost", 
			"proxyport" : 3128 
		}, 
		"index" : { 
			"index" : "dnb", 
			"type" : "authorities" 
		} 
	}'

Example of indexed Dublin Core
------------------------------

The following example illustrates an indexed Dublin Core resource from Universität zu Köln.

	{
	   "_source" : {
	      "dc:creator" : "Hölscher, Georg",
	      "dc:format" : "image/jpg",
	      "dc:identifier" : [
	         "RHG2077",
	         "823532",
	         "HT008931035",
	         "http://www.ub.uni-koeln.de/permalink/2012/01/rheinmono,8522"
	      ],
	      "dc:title" : "Köln am Rhein",
	      "dc:type" : "Image",
	      "dc:date" : "1903"
	   },
	   "_score" : 1,
	   "_index" : "oai",
	   "_id" : "oai:contentdm.ub.uni-koeln.de:rheinmono/8522",
	   "_type" : "default"
	}

Example of indexed RDF
----------------------

The following example illustrates an indexed RDF resource from Deutsche Nationalbibliothek.

	        {
	            "_score" : 1,
	            "_index" : "dnb",
	            "_id" : "http://d-nb.info/gnd/1019116501",
	            "_type" : "authorities"
	            "_source" : {
	               "gnd:biographicalOrHistoricalInformation" : "von Mittweida, hohe Schule in Leipzig (Universität?)",
	               "gnd:publication" : "Den Anfang die Früchte der Weisheit zu sammlen, als der Wohledle und Wohlgelahrte Herr, Herr Johann August König, von Mittweyda, auf der hohen Schule zu Leipzig den 25 Tag des Hornungs 1751 auf einem philosophischen Ehrentage die Meisterwürde rühmlich erlangte, besinget Ul*. - 1751",
	               "rdf:type" : "http://d-nb.info/standards/elementset/gnd#DifferentiatedPerson",
	               "gnd:variantNameEntityForThePerson" : [
	                  {
	                     "gnd:surname" : "König",
	                     "gnd:forename" : "Johannes August"
	                  },
	                  {
	                     "gnd:surname" : "König",
	                     "gnd:forename" : "Johannes Augustus"
	                  }
	               ],
	               "gnd:oldAuthorityNumber" : "(DE-588a)1019116501",
	               "gnd:gndIdentifier" : "1019116501",
	               "gnd:preferredNameEntityForThePerson" : {
	                  "gnd:surname" : "König",
	                  "gnd:forename" : "Johann August"
	               },
	               "gnd:gender" : "http://d-nb.info/standards/vocab/gnd/Gender#male",
	               "gnd:periodOfActivity" : "1751",
	               "gnd:variantNameForThePerson" : [
	                  "König, Johannes August",
	                  "König, Johannes Augustus"
	               ],
	               "gnd:preferredNameForThePerson" : "König, Johann August"
	            }
	        }

Notes
-----

The OAI river is using the OAI module of the xbib framework, an upcoming effort to build scalable and efficient bibliographic semantic web infrastructures, which will be released soon as open source.

