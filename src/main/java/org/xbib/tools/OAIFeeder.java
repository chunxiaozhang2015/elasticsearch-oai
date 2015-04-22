package org.xbib.tools;

import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.xbib.iri.namespace.IRINamespaceContext;
import org.xbib.oai.OAIConstants;
import org.xbib.oai.OAIDateResolution;
import org.xbib.oai.client.OAIClient;
import org.xbib.oai.client.OAIClientFactory;
import org.xbib.oai.client.listrecords.ListRecordsListener;
import org.xbib.oai.client.listrecords.ListRecordsRequest;
import org.xbib.oai.rdf.RdfResourceHandler;
import org.xbib.oai.xml.SimpleMetadataHandler;
import org.xbib.pipeline.Pipeline;
import org.xbib.pipeline.PipelineProvider;
import org.xbib.rdf.RdfContentBuilder;
import org.xbib.rdf.RdfContentParams;
import org.xbib.rdf.content.RouteRdfXContent;
import org.xbib.rdf.content.RouteRdfXContentParams;
import org.xbib.util.DateUtil;
import org.xbib.util.URIUtil;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.Queues.newConcurrentLinkedQueue;
import static org.xbib.rdf.content.RdfXContentFactory.routeRdfXContentBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OAIFeeder extends TimewindowFeeder {

    private final static Logger logger = LogManager.getLogger(OAIFeeder.class.getSimpleName());

    @Override
    protected PipelineProvider<Pipeline> pipelineProvider() {
        return new PipelineProvider<Pipeline>() {
            @Override
            public Pipeline get() {
                return new OAIFeeder();
            }
        };
    }

    @Override
    protected OAIFeeder prepare() throws IOException {
        ingest = createIngest();
        String timeWindow = settings.get("timewindow") != null ?
                DateTimeFormat.forPattern(settings.get("timewindow")).print(new DateTime()) : "";
        setConcreteIndex(resolveAlias(settings.get("index") + timeWindow));
        Pattern pattern = Pattern.compile("^(.*)\\d+$");
        Matcher m = pattern.matcher(getConcreteIndex());
        setIndex(m.matches() ? m.group() : getConcreteIndex());
        logger.info("base index name = {}, concrete index name = {}",
                getIndex(), getConcreteIndex());

        Integer maxbulkactions = settings.getAsInt("maxbulkactions", 1000);
        Integer maxconcurrentbulkrequests = settings.getAsInt("maxconcurrentbulkrequests",
                Runtime.getRuntime().availableProcessors());
        ingest.maxActionsPerBulkRequest(maxbulkactions)
                .maxConcurrentBulkRequests(maxconcurrentbulkrequests);
        createIndex(getConcreteIndex());

        String[] inputs = settings.getAsArray("uri");
        if (inputs == null || inputs.length == 0) {
            throw new IllegalArgumentException("no parameter 'uri' given");
        }
        input = newConcurrentLinkedQueue();
        for (String uri : inputs) {
            input.offer(URI.create(uri));
        }
        return this;
    }

    @Override
    public void process(URI uri) throws Exception {
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
        RdfContentParams params = RdfContentParams.EMPTY;
        return new RdfResourceHandler(params);
    }

    protected SimpleMetadataHandler newMetadataHandler() {
        return new DCMetadataHandler();
    }

    public class DCMetadataHandler extends SimpleMetadataHandler {

        private final IRINamespaceContext namespaceContext;

        private RdfResourceHandler handler;

        public DCMetadataHandler() {
            namespaceContext = IRINamespaceContext.newInstance();
            namespaceContext.addNamespace("oai_dc", "http://www.openarchives.org/OAI/2.0/oai_dc/");
            namespaceContext.addNamespace("dc", "http://purl.org/dc/elements/1.1/");
        }

        @Override
        public void startDocument() throws SAXException {
            this.handler = rdfResourceHandler();
            handler.setDefaultNamespace("oai_dc", "http://www.openarchives.org/OAI/2.0/oai_dc/");
            handler.startDocument();
        }

        @Override
        public void endDocument() throws SAXException {
            handler.endDocument();
            try {
                RouteRdfXContentParams params = new RouteRdfXContentParams(namespaceContext,
                        getConcreteIndex(), getType());
                params.setHandler(new RouteRdfXContent.RouteHandler() {
                    @Override
                    public void complete(String content, RouteRdfXContentParams p) throws IOException {
                        ingest.index(p.getIndex(), p.getType(), getHeader().getIdentifier(), content);
                    }
                });
                RdfContentBuilder builder = routeRdfXContentBuilder(params);
                builder.receive(handler.getResource());
                if (settings.getAsBoolean("mock", false)) {
                    logger.info("{}", builder.string());
                }
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
