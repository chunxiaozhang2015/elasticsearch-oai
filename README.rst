.. image:: ../../../elasticsearch-river-oai/raw/master/src/site/resources/OA200.gif

Image Copyright (C) `http://www.openarchives.org/`

OAI Plugin for Elasticsearch
============================

The `Open Archives Initiative - Protocol for Metadata Harvesting (OAI-PMH) <http://www.openarchives.org/pmh/>`_
plugin allows to harvest metadata into Elasticsearch.

The plugin allows to harvest DC / XML / RDF formats from OAI data providers (OAI servers).
A list of OAI servers can be found `here <http://www.openarchives.org/Register/BrowseSites>`_.
The metadata is internally represented as resources, using Resource Description Framework (RDF) of
the W3C Semantic Web Initiative, before the resources are serialized into JSON-LD for
Elasticsearch schema-less indexing.

Build status
------------

.. image:: https://travis-ci.org/jprante/elasticsearch-river-oai.png

Prerequisites
-------------

- Java 8 JDK

Versions
--------

=============  ===========  =================
ES version     Plugin       Release date
-------------  -----------  -----------------
1.1.0          1.1.0.0      Apr 20, 2014
=============  ===========  =================

Installation
------------

    ./bin/plugin --install oai --url http://xbib.org/repository/org/xbib/elasticsearch/plugin/elasticsearch-river-oai/1.1.0.0/elasticsearch-river-oai-1.1.0.0.zip

Do not forget to restart the node after installing.

Checksum
--------

File: elasticsearch-river-oai-1.1.0.0.zip
SHA1: 2bd2d0813880d5a6426e2c6a34dc9b05945f12ba

Project docs
------------

The Maven project site is available at `Github <http://jprante.github.io/elasticsearch-river-oai>`_

Issues
------

All feedback is welcome! If you find issues, please post them at `Github <https://github.com/jprante/elasticsearch-river-oai/issues>`_

Documentation
=============

This `plugin <http://www.elasticsearch.org/guide/reference/modules/plugins.html>`_
for Elasticsearch can run as a river or a feeder.

Starting a river instance
-------------------------

A `river <http://www.elasticsearch.org/guide/reference/river/>`_ runs within a running node
of an Elasticsearch cluster and pulls data.

Setting up a river is as simple as executing the following command::

    curl -XPUT 'localhost:9200/_river/my_arxiv_river/_meta' -d '{
      "type" : "oai",
      "oai" : {
        "input" : [
            "http://export.arxiv.org/oai2?verb=ListRecords&metadataPrefix=oai_dc&from=2000-01-01&until=2015-01-01"
        ]
      }
    }'

This call will create a river that harvests all the Dublin Core metadata
from the arXiv, the first and most prominent public OAI data provider. This will take
approximately 50 hours, because arXiv forces a pause of 20 seconds between every 1000
harvested documents.

A full example would be::

    curl -XPUT 'localhost:9200/_river/my_arxiv_river/_meta' -d '{
      "type" : "oai",
      "oai" : {
        "input" : [
            "http://export.arxiv.org/oai2?verb=ListRecords&metadataPrefix=oai_dc&from=2000-01-01&until=2015-01-01"
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
    }'


Starting a feeder instance
--------------------------

A feeder is a standalone plugin that can push data into a remote Elasticsearch
cluster and runs outside an Elasticsearch node.

Setting up a standalone feeder is also simple. Download Elasticsearch and install it
as you would for a node. Install the plugin as you would for a river. Instead of
starting the node, change into the `plugins/oai` folder and execute::

    bash bin/feeder/arxiv/arxiv.sh

where the shell script has the content::

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
        "index" : "arxiv",
        "type" : "arxiv",
        "shards" : 1,
        "replica" : 0,
        "maxbulkactions" : 1000,
        "maxconcurrentbulkrequests" : 1,
        "trace" : false,
        "scrubxml" : false,
        "elasticsearch" : "es://localhost:9300?es.cluster.name=elasticsearch",
        "client" : "bulk"
    }
    ' | ${java} \
        -cp $(pwd):$(pwd)/\*:$(pwd)/../../lib/\* \
        org.xbib.elasticsearch.plugin.feeder.Runner \
        org.xbib.elasticsearch.plugin.feeder.oai.OAIFeeder


Before running, you should configure where your Java 8 installation is located.

The logging can be controlled by the ``log4j.properties`` file in the plugin folder.

Parameters
----------

input - a list of URLs for harvesting

concurrency - how many URLs should be processed simultaneously

handler - ``xml`` for XML metadata content, ``rdf`` for RDF/XML

index - the name of the Elasticsearch index

type - the name of the Elasticsearch type

shards - the number of shards of the index

replica - the replica level for the index

maxbulkactions - the maximum number of actions in a bulk request

maxconcurrentbulkrequests - the maximum number of concurrent bulk requests

trace - if ``true``, the harvested content will be logged. Default is ``false``

scrubxml - if ``true``, the harvested content will be scrubbed from invalid XML characters. Default is ``true``

elasticsearch - an URI to address an Elasticsearch node. URI parameter ``es.cluster.name`` determines the cluster name

client - ``bulk`` selects the default Elasticsearch BulkProcessor API, ``ingest`` selects an xbib implementation


License
=======

Elasticsearch OAI Plugin

Copyright (C) 2014 Jörg Prante and xbib organization

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.