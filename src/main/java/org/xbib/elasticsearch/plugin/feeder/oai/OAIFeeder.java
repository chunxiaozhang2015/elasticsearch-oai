package org.xbib.elasticsearch.plugin.feeder.oai;

import org.elasticsearch.common.settings.loader.JsonSettingsLoader;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.plugin.feeder.AbstractFeeder;
import org.xbib.elasticsearch.plugin.feeder.DereferencingContentBuilder;
import org.xbib.elasticsearch.plugin.feeder.Feeder;
import org.xbib.elasticsearch.rdf.ResourceSink;
import org.xbib.elasticsearch.support.client.bulk.BulkTransportClient;
import org.xbib.elasticsearch.support.client.ingest.IngestTransportClient;
import org.xbib.iri.IRI;
import org.xbib.logging.Logger;
import org.xbib.logging.LoggerFactory;
import org.xbib.oai.OAIDateResolution;
import org.xbib.oai.client.OAIClient;
import org.xbib.oai.client.OAIClientFactory;
import org.xbib.oai.listrecords.ListRecordsListener;
import org.xbib.oai.listrecords.ListRecordsRequest;
import org.xbib.oai.rdf.RdfOutput;
import org.xbib.oai.rdf.RdfResourceHandler;
import org.xbib.oai.xml.MetadataHandler;
import org.xbib.pipeline.Pipeline;
import org.xbib.pipeline.PipelineProvider;
import org.xbib.pipeline.PipelineRequest;
import org.xbib.rdf.Resource;
import org.xbib.rdf.Triple;
import org.xbib.rdf.content.ContentBuilder;
import org.xbib.rdf.context.IRINamespaceContext;
import org.xbib.rdf.context.ResourceContext;
import org.xbib.rdf.io.TripleListener;
import org.xbib.rdf.io.rdfxml.RdfXmlReader;
import org.xbib.rdf.io.xml.XmlHandler;
import org.xbib.rdf.simple.SimpleLiteral;
import org.xbib.rdf.simple.SimpleResourceContext;
import org.xbib.rdf.simple.SimpleTriple;
import org.xbib.rdf.types.XSD;
import org.xbib.util.DateUtil;
import org.xbib.util.URIUtil;
import org.xbib.xml.XMLNS;
import org.xbib.xml.XSI;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.util.Date;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class OAIFeeder<T, R extends PipelineRequest, P extends Pipeline<T, R>>
        extends AbstractFeeder<T, R, P> {

    private final static Logger logger = LoggerFactory.getLogger(OAIFeeder.class.getSimpleName());

    protected ResourceSink sink;

    public OAIFeeder() {
    }

    public OAIFeeder(OAIFeeder feeder) {
        super(feeder);
        this.sink = feeder.sink;
    }

    @Override
    public PipelineProvider<P> pipelineProvider() {
        return new PipelineProvider<P>() {
            @Override
            public P get() {
                return (P) new OAIFeeder(OAIFeeder.this);
            }
        };
    }

    @Override
    public String getType() {
        return "oai";
    }

    public Feeder<T, R, P> beforeRun() throws IOException {
        if (spec == null) {
            throw new IllegalArgumentException("no spec?");
        }
        if (settings == null) {
            throw new IllegalArgumentException("no settings?");
        }
        if (ingest == null) {
            Integer maxbulkactions = settings.getAsInt("maxbulkactions", 1000);
            Integer maxconcurrentbulkrequests = settings.getAsInt("maxconcurrentbulkrequests",
                    Runtime.getRuntime().availableProcessors());
            ByteSizeValue maxvolume = settings.getAsBytesSize("maxbulkvolume", ByteSizeValue.parseBytesSizeValue("10m"));
            TimeValue maxrequestwait = settings.getAsTime("maxrequestwait", TimeValue.timeValueSeconds(60));
            ingest = "ingest".equals(settings.get("client")) ?
                    new IngestTransportClient()
                    : new BulkTransportClient();
            ingest.maxActionsPerBulkRequest(maxbulkactions)
                    .maxConcurrentBulkRequests(maxconcurrentbulkrequests)
                    .maxVolumePerBulkRequest(maxvolume)
                    .maxRequestWait(maxrequestwait);
            ingest.newClient(URI.create(settings.get("elasticsearch")));
            this.sink = new ResourceSink(ingest);
        }
        // create queue
        super.beforeRun();
        return this;
    }

    @Override
    public void executeTask(Map<String, Object> map) throws Exception {
        logger.info("{}", map);

        String index = settings.get("index", "oai");
        String type = settings.get("type", "oai");
        if (spec.containsKey("index_settings")) {
            try {
                ingest.setSettings(settingsBuilder().put(new JsonSettingsLoader()
                        .load(jsonBuilder().map((Map<String, Object>) spec.get("index_settings")).string()))
                        .build());
                if (spec.containsKey("type_mapping")) {
                    ingest.addMapping(type,
                            jsonBuilder().map((Map<String, Object>) spec.get("type_mapping")).string());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(),e);
            }
        }
        ingest.newIndex(index);

        URI uri = URI.create(map.get("url").toString());
        Map<String, String> params = URIUtil.parseQueryString(uri);
        String server = uri.toString();
        String metadataPrefix = params.get("metadataPrefix");
        String set = params.get("set");
        Date from = "now".equals(params.get("from")) ? new Date() :
                DateUtil.parseDateISO(params.get("from"));
        Date until = "now".equals(params.get("until")) ? new Date() :
                DateUtil.parseDateISO(params.get("until"));
        final OAIClient oaiClient = OAIClientFactory.newClient(server);
        oaiClient.setTimeout(settings.getAsInt("timeout", 60000));

        ListRecordsRequest request = oaiClient.newListRecordsRequest()
                .setMetadataPrefix(metadataPrefix)
                .setSet(set)
                .setFrom(from, OAIDateResolution.DAY)
                .setUntil(until, OAIDateResolution.DAY);

        do {
            try {
                request.addHandler("xml".equals(settings.get("handler")) ?
                        xmlMetadataHandler() : rdfMetadataHandler());
                ListRecordsListener listener = new ListRecordsListener(request);
                listener.setScrubCharacters(settings.getAsBoolean("scrubxml", true));
                request.prepare().execute(listener).waitFor();
                if (listener.getResponse() != null) {
                    StringWriter w = new StringWriter();
                    try {
                        listener.getResponse().to(w);
                    } catch (IOException e) {
                        logger.error(e.getMessage(), e);
                    } finally {
                        if (settings.getAsBoolean("trace", false)) {
                            logger.info(w.toString());
                        }
                    }
                    request = oaiClient.resume(request, listener.getResumptionToken());
                    if (request != null && request.getResumptionToken() != null) {
                        logger.info("{} received resumption token {}", server, request.getResumptionToken().toString());
                    }
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                request = null;
            }
        } while (request != null && !isInterrupted());
        oaiClient.close();
    }

    protected MetadataHandler xmlMetadataHandler() {
        IRINamespaceContext namespaceContext = IRINamespaceContext.getInstance();
        ResourceContext<Resource> resourceContext = new SimpleResourceContext()
                .setContentBuilder(contentBuilder(namespaceContext))
                .setNamespaceContext(namespaceContext);
        resourceContext.setNamespaceContext(IRINamespaceContext.getInstance());
        RdfResourceHandler rdfResourceHandler = new RdfResourceHandler(resourceContext);
        return new XmlMetadataHandler()
                .setAttributeParsing(true)
                .setHandler(rdfResourceHandler)
                .setResourceContext(rdfResourceHandler.resourceContext())
                .setOutput(new ElasticOut());
    }

    protected MetadataHandler rdfMetadataHandler() {
        IRINamespaceContext namespaceContext = IRINamespaceContext.getInstance();
        ResourceContext<Resource> resourceContext = new SimpleResourceContext()
                .setContentBuilder(contentBuilder(namespaceContext))
                .setNamespaceContext(IRINamespaceContext.getInstance());
        RdfXmlReader reader = new RdfXmlReader().setTripleListener(new GeoJSONFilter(resourceContext));
        return new XmlMetadataHandler()
                .setAttributeParsing(false)
                .setHandler(reader.getHandler())
                .setResourceContext(resourceContext)
                .setOutput(new ElasticOut());
    }

    protected ContentBuilder contentBuilder(IRINamespaceContext namespaceContext) {
        return new DereferencingContentBuilder(namespaceContext,
                ingest.client(),
                settings.get("deref_index"),
                settings.get("deref_type"),
                settings.get("deref_prefix"),
                settings.getAsArray("deref_field"));
    }

    class XmlMetadataHandler extends MetadataHandler {

        private XmlHandler handler;

        private ResourceContext resourceContext;

        private RdfOutput output;

        private boolean attributeParsingEnabled;

        private boolean attributes;

        private String uri;

        private String qname;

        public XmlMetadataHandler setHandler(XmlHandler handler) {
            this.handler = handler;
            this.handler.setDefaultNamespace("oai_dc", "http://www.openarchives.org/OAI/2.0/oai_dc/");
            return this;
        }

        public XmlMetadataHandler setResourceContext(ResourceContext resourceContext) {
            this.resourceContext = resourceContext;
            return this;
        }

        public XmlMetadataHandler setAttributeParsing(boolean attributeParsingEnabled) {
            this.attributeParsingEnabled = attributeParsingEnabled;
            return this;
        }

        public XmlMetadataHandler setOutput(RdfOutput output) {
            this.output = output;
            return this;
        }

        @Override
        public void startDocument() throws SAXException {
            handler.startDocument();
        }

        @Override
        public void endDocument() throws SAXException {
            handler.endDocument();
            String identifier = getHeader().getIdentifier();
            try {
                if (resourceContext.getResource() != null) {
                    IRI iri = IRI.builder().scheme("http")
                            .host(getSettings().get("index", "oai"))
                            .query(getSettings().get("type", "oai"))
                            .fragment(identifier).build();
                    resourceContext.getResource().id(iri);
                }
                output.output(resourceContext);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }

        @Override
        public void startPrefixMapping(String prefix, String nsURI) throws SAXException {
            handler.startPrefixMapping(prefix, nsURI);
            resourceContext.getNamespaceContext().addNamespace(prefix, nsURI);
            if ("".equals(prefix)) {
                handler.setDefaultNamespace(getSettings().get("type", "oai"), nsURI);
                resourceContext.getNamespaceContext().addNamespace(getSettings().get("type", "oai"), nsURI);
            }
        }

        @Override
        public void endPrefixMapping(String string) throws SAXException {
            handler.endPrefixMapping(string);
        }

        @Override
        public void startElement(String ns, String localname, String string2, Attributes atrbts) throws SAXException {
            handler.startElement(ns, localname, string2, atrbts);
            attributes = false;
            if (attributeParsingEnabled) {
                uri = ns;
                this.qname = string2;
                for (int i = 0; i < atrbts.getLength(); i++) {
                    String uri = atrbts.getURI(i);
                    String qname = atrbts.getQName(i);
                    char[] ch = atrbts.getValue(i).toCharArray();
                    if (!qname.startsWith(XMLNS.NS_PREFIX) && !XSI.NS_URI.equals(uri)) {
                        String localName = "attr_" + atrbts.getLocalName(i);
                        int pos = qname.indexOf(':');
                        String attr_qname = pos > 0 ? qname.substring(0, pos + 1) + localName : localName;
                        handler.startElement(uri, localName, attr_qname, emptyAttributes);
                        handler.characters(ch, 0, ch.length);
                        handler.endElement(uri, localName, attr_qname);
                        attributes = true;
                    } else if (qname.startsWith(XMLNS.NS_PREFIX) && !XSI.NS_URI.equals(uri)) {
                        int pos = qname.indexOf(':');
                        String prefix = pos > 0 ? qname.substring(pos + 1) : "xmlns";
                        handler.startElement(XMLNS.NS_URI, "_namespace", XMLNS.NS_PREFIX + ":_namespace", emptyAttributes);
                        handler.startElement(XMLNS.NS_URI, "_prefix", XMLNS.NS_PREFIX + ":_prefix", emptyAttributes);
                        char[] p = prefix.toCharArray();
                        handler.characters(p, 0, p.length);
                        handler.endElement(XMLNS.NS_URI, "_prefix", XMLNS.NS_PREFIX + ":_prefix");
                        handler.startElement(XMLNS.NS_URI, "_uri", XMLNS.NS_PREFIX + ":_uri", emptyAttributes);
                        handler.characters(ch, 0, ch.length);
                        handler.endElement(XMLNS.NS_URI, "_uri", XMLNS.NS_PREFIX + ":_uri");
                        handler.endElement(XMLNS.NS_URI, "_namespace", XMLNS.NS_PREFIX + ":_namespace");
                        attributes = true;
                    }
                }
            }
        }

        @Override
        public void endElement(String ns, String localname, String string2) throws SAXException {
            handler.endElement(ns, localname, string2);
        }

        @Override
        public void characters(char[] chars, int i, int i1) throws SAXException {
            if (attributes) {
                handler.startElement(uri, "_value", qname, emptyAttributes);
            }
            handler.characters(chars, i, i1);
            if (attributes) {
                handler.endElement(uri, "_value", qname);
            }
        }

        private final Attributes emptyAttributes = new AttributesImpl();
    }

    class ElasticOut extends RdfOutput {
        @Override
        public RdfOutput output(ResourceContext<Resource> resourceContext) throws IOException {
            if (resourceContext == null) {
                return this;
            }
            if (resourceContext.getResources() == null) {
                // single document
                sink.output(resourceContext, resourceContext.getResource(), resourceContext.getContentBuilder());
            } else {
                for (Resource resource : resourceContext.getResources()) {
                    // multiple documents. Rewrite IRI for ES index/type addressing
                    String index = getSettings().get("index", "oai");
                    String type = getSettings().get("type", "oai");
                    if (index.equals(resource.id().getHost())) {
                        IRI iri = IRI.builder().scheme("http").host(index).query(type)
                                .fragment(resource.id().getFragment()).build();
                        resource.add("iri", resource.id().getFragment());
                        resource.id(iri);
                    } else {
                        IRI iri = IRI.builder().scheme("http").host(index).query(type)
                                .fragment(resource.id().toString()).build();
                        resource.add("iri", resource.id().toString());
                        resource.id(iri);
                    }
                    sink.output(resourceContext, resource, resourceContext.getContentBuilder());
                }
            }
            return this;
        }
    }

    class GeoJSONFilter implements TripleListener {

        ResourceContext<Resource> resourceContext;

        String lat;

        String lon;

        GeoJSONFilter(ResourceContext<Resource> resourceContext) {
            this.resourceContext = resourceContext;
        }

        @Override
        public TripleListener begin() {
            return this;
        }

        @Override
        public TripleListener startPrefixMapping(String prefix, String uri) {
            return this;
        }

        @Override
        public TripleListener endPrefixMapping(String prefix) {
            return this;
        }

        @Override
        public TripleListener newIdentifier(IRI identifier) {
            return this;
        }

        @Override
        public TripleListener triple(Triple triple) {
            resourceContext.triple(triple);
            if (triple.predicate().id().toString().equals(GEO_LAT.toString())) {
                lat = triple.object().toString();
            } else if (triple.predicate().id().toString().equals(GEO_LON.toString())) {
                lon = triple.object().toString();
            }
            if (lat != null && lon != null && !"0.0".equals(lat) && !"0.0".equals(lon)) {
                // create GeoJSON array
                resourceContext.triple(new SimpleTriple(triple.subject(),
                        "location",
                        new SimpleLiteral().object(lon).type(XSD.DOUBLE)));
                resourceContext.triple(new SimpleTriple(triple.subject(),
                        "location",
                        new SimpleLiteral().object(lat).type(XSD.DOUBLE)));
                lon = null;
                lat = null;
            }
            return this;
        }

        @Override
        public TripleListener end() {
            return this;
        }
    }

    private final static IRI GEO_LAT = IRI.create("http://www.w3.org/2003/01/geo/wgs84_pos#lat");

    private final static IRI GEO_LON = IRI.create("http://www.w3.org/2003/01/geo/wgs84_pos#long");


}
