
package org.xbib.tools;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.xbib.elasticsearch.helper.client.LongAdderIngestMetric;
import org.xbib.oai.OAIConstants;
import org.xbib.oai.OAIDateResolution;
import org.xbib.oai.client.OAIClient;
import org.xbib.oai.client.OAIClientFactory;
import org.xbib.oai.client.listrecords.ListRecordsListener;
import org.xbib.oai.client.listrecords.ListRecordsRequest;
import org.xbib.oai.rdf.RdfResourceHandler;
import org.xbib.oai.xml.SimpleMetadataHandler;
import org.xbib.iri.namespace.IRINamespaceContext;
import org.xbib.rdf.RdfContentBuilder;
import org.xbib.rdf.RdfContentParams;
import org.xbib.rdf.content.RouteRdfXContentParams;
import org.xbib.rdf.io.ntriple.NTripleContentParams;
import org.xbib.util.DateUtil;
import org.xbib.util.URIUtil;
import org.xbib.util.concurrent.URIWorkerRequest;
import org.xbib.util.concurrent.Worker;
import org.xbib.util.concurrent.WorkerProvider;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.util.Date;
import java.util.Map;

import static org.xbib.rdf.content.RdfXContentFactory.routeRdfXContentBuilder;

/**
 * Harvest from OAI and feed to Elasticsearch
 */
public class OAIFeeder extends TimewindowFeeder {

    private final static Logger logger = LogManager.getLogger(OAIFeeder.class);

    private static String index;

    private static String concreteIndex;

    protected void setIndex(String index) {
        this.index = index;
    }

    protected String getIndex() {
        return index;
    }

    protected void setConcreteIndex(String concreteIndex) {
        this.concreteIndex = concreteIndex;
    }

    protected String getConcreteIndex() {
        return concreteIndex;
    }

    @Override
    protected WorkerProvider<Worker<URIWorkerRequest>> provider() {
        return OAIFeeder::new;
    }

    @Override
    protected void prepareSink() throws IOException {
        if (ingest == null) {
            ingest = createIngest();
            Integer maxbulkactions = settings.getAsInt("maxbulkactions", 1000);
            Integer maxconcurrentbulkrequests = settings.getAsInt("maxconcurrentbulkrequests",
                    Runtime.getRuntime().availableProcessors());
            ingest.maxActionsPerRequest(maxbulkactions)
                    .maxConcurrentRequests(maxconcurrentbulkrequests);
            ingest.init(Settings.settingsBuilder()
                    .put("cluster.name", settings.get("elasticsearch.cluster"))
                    .put("host", settings.get("elasticsearch.host"))
                    .put("port", settings.getAsInt("elasticsearch.port", 9300))
                    .put("sniff", settings.getAsBoolean("elasticsearch.sniff", false))
                    .put("autodiscover", settings.getAsBoolean("elasticsearch.autodiscover", false))
                    .build(), new LongAdderIngestMetric());
        }
        super.prepareSink();
    }

    @Override
    protected void process(URI uri) throws Exception {
        Map<String, String> params = URIUtil.parseQueryString(uri);
        String server = uri.toString();
        String verb = params.get("verb");
        String metadataPrefix = params.get("metadataPrefix");
        String set = params.get("set");
        Date from = DateUtil.parseDateISO(params.get("from"));
        Date until = DateUtil.parseDateISO(params.get("until"));
        final OAIClient client = OAIClientFactory.newClient(server);
        client.setTimeout(settings.getAsInt("timeout", 60000));
        if (!verb.equals(OAIConstants.LIST_RECORDS)) {
            logger.warn("no verb {}, returning", OAIConstants.LIST_RECORDS);
            return;
        }
        ListRecordsRequest request = client.newListRecordsRequest()
                .setMetadataPrefix(metadataPrefix)
                .setSet(set)
                .setFrom(from, OAIDateResolution.DAY)
                .setUntil(until, OAIDateResolution.DAY);
        do {
            try {
                request.addHandler(newMetadataHandler());
                ListRecordsListener listener = new ListRecordsListener(request);
                logger.info("OAI request: {}", request);
                request.prepare().execute(listener).waitFor();
                if (listener.getResponse() != null) {
                    logger.debug("got OAI response");
                    StringWriter w = new StringWriter();
                    listener.getResponse().to(w);
                    logger.debug("{}", w);
                    request = client.resume(request, listener.getResumptionToken());
                } else {
                    logger.debug("no valid OAI response");
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                request = null;
            }
        } while (request != null);
        client.close();
    }

    protected RdfResourceHandler rdfResourceHandler() {
        RdfContentParams params = NTripleContentParams.DEFAULT_PARAMS;
        return new RdfResourceHandler(params);
    }

    protected SimpleMetadataHandler newMetadataHandler() {
        return new OAISimpleMetadataHandler();
    }

    protected String map(String id, String content) throws IOException {
        return content;
    }

    @Override
    public void newRequest(Worker worker, URIWorkerRequest request) {
        try {
            URI uri = request.get();
            logger.info("processing URI {}", uri);
            process(uri);
        } catch (Throwable ex) {
            logger.error(request.get() + ": error while processing input: " + ex.getMessage(), ex);
        }
    }

    public class OAISimpleMetadataHandler extends SimpleMetadataHandler {

        private final IRINamespaceContext namespaceContext;

        private RdfResourceHandler handler;

        public OAISimpleMetadataHandler() {
            namespaceContext = IRINamespaceContext.newInstance();
            namespaceContext.addNamespace("", "http://www.openarchives.org/OAI/2.0/oai_dc/");
            namespaceContext.addNamespace("dc", "http://purl.org/dc/elements/1.1/");
        }

        @Override
        public void startDocument() throws SAXException {
            this.handler = rdfResourceHandler();
            handler.setDefaultNamespace("", "http://www.openarchives.org/OAI/2.0/oai_dc/");
            handler.startDocument();
        }

        @Override
        public void endDocument() throws SAXException {
            handler.endDocument();
            try {
                RouteRdfXContentParams params = new RouteRdfXContentParams(namespaceContext,
                        getConcreteIndex(), getType());
                params.setHandler((content, p) -> {
                    content = map(getHeader().getIdentifier(), content);
                    if (settings.getAsBoolean("mock", false)) {
                        logger.info("{}", content);
                    } else {
                        ingest.index(p.getIndex(), p.getType(), getHeader().getIdentifier(), content);
                    }
                });
                RdfContentBuilder builder = routeRdfXContentBuilder(params);
                builder.receive(handler.getResource());
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                throw new SAXException(e);
            }
        }

        @Override
        public void startPrefixMapping(String string, String string1) throws SAXException {
            handler.startPrefixMapping(string, string1);
        }

        @Override
        public void endPrefixMapping(String string) throws SAXException {
            handler.endPrefixMapping(string);
        }

        @Override
        public void startElement(String ns, String localname, String string2, Attributes atrbts) throws SAXException {
            handler.startElement(ns, localname, string2, atrbts);
        }

        @Override
        public void endElement(String ns, String localname, String string2) throws SAXException {
            handler.endElement(ns, localname, string2);
        }

        @Override
        public void characters(char[] chars, int i, int i1) throws SAXException {
            handler.characters(chars, i, i1);
        }
    }

}
