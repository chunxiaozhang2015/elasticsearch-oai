/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.river.oai.support;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.ClientIngest;
import org.elasticsearch.client.support.ClientIngestSupport;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.xbib.date.DateUtil;
import org.xbib.elasticsearch.ElasticsearchResourceSink;
import org.xbib.io.NullWriter;
import org.xbib.io.http.netty.HttpResponse;
import org.xbib.iri.IRI;
import org.xbib.oai.ListRecordsRequest;
import org.xbib.oai.ListRecordsResponse;
import org.xbib.oai.MetadataPrefixService;
import org.xbib.oai.MetadataReader;
import org.xbib.oai.ResumptionToken;
import org.xbib.oai.client.OAIClient;
import org.xbib.oai.client.OAIClientFactory;
import org.xbib.rdf.Triple;
import org.xbib.rdf.context.IRINamespaceContext;
import org.xbib.rdf.context.ResourceContext;
import org.xbib.rdf.io.TripleListener;
import org.xbib.rdf.io.XmlTriplifier;
import org.xbib.rdf.io.xml.XmlResourceHandler;
import org.xbib.rdf.simple.SimpleResourceContext;
import org.xbib.text.CharUtils;
import org.xbib.text.UrlEncoding;
import org.xbib.xml.transform.StylesheetTransformer;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import javax.xml.namespace.QName;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class Harvester implements Runnable {

    private final ESLogger logger = Loggers.getLogger(Harvester.class);

    private String oaiUrl;
    private String oaiSet;
    private String oaiMetadataPrefix;
    private String oaiFrom;
    private String oaiUntil;
    private String oaiProxyHost;
    private int oaiProxyPort;
    private TimeValue oaiPoll;
    private TimeValue oaiTimeout;

    private Client client;
    private String riverName;
    private String riverIndexName;
    private String indexName;
    private String typeName;
    private int maxBulkActions;
    private int maxConcurrentRequests;

    private volatile boolean closed;
    private volatile boolean failure;

    public Harvester oaiUrl(String oaiUrl) {
        this.oaiUrl = oaiUrl;
        return this;
    }

    public Harvester oaiSet(String oaiSet) {
        this.oaiSet = oaiSet;
        return this;
    }

    public Harvester oaiMetadataPrefix(String oaiMetadataPrefix) {
        this.oaiMetadataPrefix = oaiMetadataPrefix;
        return this;
    }

    public Harvester oaiFrom(String oaiFrom) {
        this.oaiFrom = oaiFrom;
        return this;
    }

    public Harvester oaiUntil(String oaiUntil) {
        this.oaiUntil = oaiUntil;
        return this;
    }

    public Harvester oaiProxyHost(String oaiProxyHost) {
        this.oaiProxyHost = oaiProxyHost;
        return this;
    }

    public Harvester oaiProxyPort(int oaiProxyPort) {
        this.oaiProxyPort = oaiProxyPort;
        return this;
    }

    public Harvester oaiPoll(TimeValue oaiPoll) {
        this.oaiPoll = oaiPoll;
        return this;
    }

    public Harvester oaiTimeout(TimeValue oaiTimeout) {
        this.oaiTimeout = oaiTimeout;
        return this;
    }

    public Harvester client(Client client) {
        this.client = client;
        return this;
    }

    public Harvester riverName(String riverName) {
        this.riverName = riverName;
        return this;
    }

    public Harvester riverIndexName(String riverIndexName) {
        this.riverIndexName = riverIndexName;
        return this;
    }

    public Harvester index(String indexName) {
        this.indexName = indexName;
        return this;
    }

    public Harvester type(String typeName) {
        this.typeName = typeName;
        return this;
    }

    public Harvester maxBulkActions(int maxBulkActions) {
        this.maxBulkActions = maxBulkActions;
        return this;
    }

    public Harvester maxConcurrentRequests(int maxConcurrentRequests) {
        this.maxConcurrentRequests = maxConcurrentRequests;
        return this;
    }

    public void setClose(boolean close) {
        this.closed = close;
    }

    @Override
    public void run() {
        logger.info("starting OAI harvester: URL [{}], set [{}], metadataPrefix [{}], from [{}], until [{}], indexing to [{}]/[{}], poll [{}]",
                oaiUrl, oaiSet, oaiMetadataPrefix, oaiFrom, oaiUntil, indexName, typeName, oaiPoll);

        final ClientIngest ingest = new ClientIngestSupport(client, indexName, typeName,
                maxBulkActions, maxConcurrentRequests);
        final ResourceContext context = new SimpleResourceContext();
        final ElasticsearchResourceSink sink = new ElasticsearchResourceSink(ingest);

        final OAIClient oaiClient = OAIClientFactory.getClient(oaiUrl);
        if (oaiProxyHost != null) {
            oaiClient.setProxy(oaiProxyHost, oaiProxyPort);
        }
        if (oaiTimeout.millis() > 0) {
            oaiClient.setTimeout(oaiTimeout.millis());
        }
        while (true) {
            if (closed) {
                return;
            }
            boolean shouldHarvest = false;
            boolean delay = true;
            final ListRecordsRequest request = new RiverListRecordsRequest(oaiClient.getURI(), oaiSet, oaiMetadataPrefix);
            request.setFrom(DateUtil.parseDateISO(oaiFrom));
            request.setUntil(DateUtil.parseDateISO(oaiUntil));
            try {
                client.admin().indices().prepareRefresh(riverIndexName).execute().actionGet();
                GetResponse get = client.prepareGet(riverIndexName, riverName, "_last").execute().actionGet();
                if (!get.exists()) {
                    // first invocation
                    shouldHarvest = true;
                } else {
                    // retrieve last OAI state (dates, resumption token)
                    Map<String, Object> oaiState = (Map<String, Object>) get.sourceAsMap().get("oai");
                    if (oaiState != null) {
                        Object lastHttpStatus = oaiState.get("last_http_status");
                        int lastHttpStatusCode;
                        if (lastHttpStatus instanceof Integer) {
                            lastHttpStatusCode = (Integer) lastHttpStatus;
                            if (lastHttpStatusCode == 200) {
                                Object resumptionToken = oaiState.get("resumptionToken");
                                if (resumptionToken != null) {
                                    ResumptionToken token = ResumptionToken.newToken(resumptionToken.toString());
                                    request.setResumptionToken(token);
                                    shouldHarvest = true;
                                } else {
                                    Object lastFrom = oaiState.get("last_from");
                                    Object lastUntil = oaiState.get("last_until");
                                    if (lastFrom == null && lastUntil == null) {
                                        // repeat harvest
                                        shouldHarvest = true;
                                    } else {
                                        // advance time period
                                        Date d1 = DateUtil.parseDateISO(lastFrom.toString());
                                        Date d2 = DateUtil.parseDateISO(lastUntil.toString());
                                        long delta1 = d2.getTime() - d1.getTime();
                                        long delta2 = delta1;
                                        // don't harvest into the future (now +/- 15 seconds)
                                        if (d2.getTime() + delta2 < new Date().getTime() - 15000L) {
                                            d1.setTime(d1.getTime() + delta1);
                                            d2.setTime(d2.getTime() + delta2);
                                            oaiFrom = DateUtil.formatDateISO(d1);
                                            request.setFrom(d1);
                                            oaiUntil = DateUtil.formatDateISO(d2);
                                            request.setUntil(d2);
                                            shouldHarvest = true;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn("failed to get last OAI state, throttling...", e);
                try {
                    Thread.sleep(60000);
                } catch (InterruptedException e1) {
                    if (closed) {
                        return;
                    }
                }
            }
            if (!shouldHarvest) {
                delay("not harvesting");
                continue;
            }
            logger.info("starting OAI harvest request to URL [{}] set [{}] metadataPrefix [{}] from [{}] until [{}] resumptionToken [{}]",
                    oaiUrl, request.getSet(), request.getMetadataPrefix(),
                    request.getFrom(), request.getUntil(), request.getResumptionToken());

            final IRINamespaceContext namespaceContext = IRINamespaceContext.newInstance();
            namespaceContext.addNamespace("oaidc", "http://www.openarchives.org/OAI/2.0/oai_dc/");
            context.newNamespaceContext(namespaceContext);

            final TripleListener tripleListener = new TripleListener() {

                @Override
                public TripleListener newIdentifier(IRI iri) {
                    context.resource().id(iri);
                    return this;
                }

                @Override
                public TripleListener triple(Triple triple) {
                    context.resource().add(triple);
                    return this;
                }
            };

            final XmlResourceHandler xmlHandler = new XmlResourceHandler(context) {

                @Override
                public boolean isResourceDelimiter(QName name) {
                    return "oai_dc".equals(name.getLocalPart());
                }

                @Override
                public void identify(QName name, String value, IRI identifier) {
                    if ("identifier".equals(name.getLocalPart()) && identifier == null) {
                        // make sure we can build an opaque IRI, whatever is out there
                        String val = UrlEncoding.encode(value, CharUtils.Profile.SCHEMESPECIFICPART.filter());
                        // host = index, query = type, fragment = id
                        IRI iri = new IRI().host(indexName).query(typeName).fragment(val).build();
                        context.resource().id(iri);
                    }
                }

                @Override
                public boolean skip(QName name) {
                    // skip dc:dc element
                    return "dc".equals(name.getLocalPart());
                }

            };

            MetadataReader metadataReader = new MetadataReader() {

                @Override
                public void startDocument() throws SAXException {
                    xmlHandler.startDocument();
                }

                @Override
                public void endDocument() throws SAXException {
                    xmlHandler.endDocument();
                    // emergency, no ID found yet
                    if (context.resource().id() == null) {
                        // host = index, query = type, fragment = id
                        // let the identifier in the header be the resource ID
                        IRI iri = new IRI().host(indexName).query(typeName).fragment(getHeader().getIdentifier()).build();
                        context.resource().id(iri);
                    }
                    // push to Elasticsearch
                    try {
                        sink.output(context);
                    } catch (IOException ex) {
                        logger.error(ex.getMessage(), ex);
                        failure = true;
                    }
                }

                @Override
                public void startPrefixMapping(String string, String string1) throws SAXException {
                    xmlHandler.startPrefixMapping(string, string1);
                }

                @Override
                public void endPrefixMapping(String string) throws SAXException {
                    xmlHandler.endPrefixMapping(string);
                }

                @Override
                public void startElement(String ns, String localname, String string2, Attributes atrbts) throws SAXException {
                    xmlHandler.startElement(ns, localname, string2, atrbts);
                }

                @Override
                public void endElement(String ns, String localname, String string2) throws SAXException {
                    xmlHandler.endElement(ns, localname, string2);
                }

                @Override
                public void characters(char[] chars, int i, int i1) throws SAXException {
                    xmlHandler.characters(chars, i, i1);
                }
            };

            oaiClient.setMetadataReader(metadataReader);

            do {
                // where we save our OAI response
                NullWriter w = new NullWriter();
                ListRecordsResponse response = new ListRecordsResponse(w);

                // find the right triplifier
                XmlTriplifier reader = MetadataPrefixService.getTriplifier(oaiMetadataPrefix);
                if (reader == null) {
                    throw new IllegalArgumentException("triplifier not found for metadata prefix '" + oaiMetadataPrefix + "'");
                }

                // prepare our reader with XML handler and triple listener
                xmlHandler.setListener(tripleListener);
                reader.setHandler(xmlHandler);

                int status = -1;
                try {
                    // TODO not sure why we need a style sheet transformer here
                    StylesheetTransformer transformer = new StylesheetTransformer("xsl");
                    oaiClient.setStylesheetTransformer(transformer);
                    oaiClient.prepareListRecords(request, response);
                    oaiClient.execute(oaiTimeout.getMillis(), TimeUnit.MILLISECONDS);
                    HttpResponse httpResponse = oaiClient.getResponse();
                    if (httpResponse == null) {
                        logger.error("no response, failure");
                        failure = true;
                        closed = true;
                    } else {
                        status = httpResponse.getStatusCode();
                        // check for some common HTTP status that should not repeat OAI requests
                        if (status == 500 || status == 400 || status == 401 || status == 403 || status == 404) {
                            logger.error("fatal HTTP status {}, aborting", status);
                            failure = true;
                            closed = true;
                        }
                        // check for OAI accept-retry extension
                        if (status == 503 && response.getExpire() > 0) {
                            try {
                                logger.info("OAI server {} wants us to wait for {} seconds",
                                        oaiUrl,
                                        response.getExpire());
                                Thread.sleep(1000L * response.getExpire());
                                delay = false; // do not wait a second time
                            } catch (InterruptedException e) {
                                // ignore
                            }
                        }
                        if (httpResponse.getThrowable() != null) {
                            logger.error(httpResponse.getThrowable().getMessage());
                            failure = true;
                            closed = true;
                        }
                    }
                } catch (IOException ex) {
                    // connection/ input/output errors
                    logger.error("OAI I/O error: {}, river aborting", ex.getMessage(), ex);
                    failure = true;
                    closed = true;
                }
                // OAI errors
                if (response.getError() != null) {
                    logger.error("OAI error: {}, river aborting", response.getError());
                    failure = true;
                    closed = true;
                }
                // save state
                try {
                    sink.flush();
                    XContentBuilder builder = jsonBuilder();
                    builder.startObject().startObject("oai");
                    if (request.getResumptionToken() != null) {
                        builder.field("resumptionToken", request.getResumptionToken());
                    }
                    if (request.getFrom() != null) {
                        builder.field("last_from", DateUtil.formatDateISO(request.getFrom()));
                    }
                    if (request.getUntil() != null) {
                        builder.field("last_until", DateUtil.formatDateISO(request.getUntil()));
                    }
                    builder.field("last_error", response.getError());
                    builder.field("last_http_status", status);
                    builder.endObject().endObject();
                    client.prepareBulk().add(indexRequest(riverIndexName).type(riverName).id("_last").source(builder)).execute().actionGet();
                } catch (IOException | ElasticSearchException ex) {
                    logger.error(ex.getMessage(), ex);
                    failure = true;
                }
            } while (request.getResumptionToken() != null && !failure && !closed);
            if (delay && !closed) {
                delay("next harvest");
            }
        }
    }

    /**
     * User-defined wait
     * @param reason the reaon for the wait
     */
    private void delay(String reason) {
        if (oaiPoll.millis() > 0L) {
            logger.info("{}, waiting {}, URL [{}] set [{}] metadataPrefix [{}]",
                    reason, oaiPoll, oaiUrl, oaiSet, oaiMetadataPrefix);
            try {
                Thread.sleep(oaiPoll.millis());
            } catch (InterruptedException e1) {
            }
        }
    }

}
