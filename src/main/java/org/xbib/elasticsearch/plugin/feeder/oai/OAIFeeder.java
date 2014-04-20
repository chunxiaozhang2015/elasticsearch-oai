
package org.xbib.elasticsearch.plugin.feeder.oai;

import org.xbib.common.unit.TimeValue;
import org.xbib.elasticsearch.plugin.feeder.Feeder;
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
import org.xbib.rdf.Triple;
import org.xbib.rdf.context.IRINamespaceContext;
import org.xbib.rdf.context.ResourceContext;
import org.xbib.rdf.io.TripleListener;
import org.xbib.rdf.io.rdfxml.RdfXmlReader;
import org.xbib.rdf.io.xml.XmlHandler;
import org.xbib.rdf.simple.SimpleResourceContext;
import org.xbib.util.DateUtil;
import org.xbib.util.URIUtil;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.util.Date;
import java.util.Map;

public class OAIFeeder extends Feeder {

    private final static Logger logger = LoggerFactory.getLogger(OAIFeeder.class.getSimpleName());

    @Override
    protected OAIFeeder prepare() throws IOException {
        super.prepare();
        return this;
    }

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
    public void process(URI uri) throws Exception {
        Map<String,String> params = URIUtil.parseQueryString(uri);
        String server = uri.toString();
        String metadataPrefix = params.get("metadataPrefix");
        String set = params.get("set");
        Date from = DateUtil.parseDateISO(params.get("from"));
        Date until = DateUtil.parseDateISO(params.get("until"));
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
                    TimeValue delay = settings.getAsTime("delay", null);
                    if (delay != null) {
                        logger.info("delay for {} seconds", delay.getSeconds());
                        Thread.sleep(delay.millis());
                    }
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                request = null;
            }
        } while (request != null);
        oaiClient.close();
    }

    protected MetadataHandler xmlMetadataHandler() {
        SimpleResourceContext resourceContext = new SimpleResourceContext();
        resourceContext.setNamespaceContext(IRINamespaceContext.getInstance());
        RdfResourceHandler rdfResourceHandler = new RdfResourceHandler(resourceContext);
        return new XmlMetadataHandler()
                .setHandler(rdfResourceHandler)
                .setResourceContext(rdfResourceHandler.resourceContext())
                .setOutput(new ElasticOut());
    }

    protected MetadataHandler rdfMetadataHandler() {
        SimpleResourceContext resourceContext = new SimpleResourceContext();
        resourceContext.setNamespaceContext(IRINamespaceContext.getInstance());
        ResourceBuilder builder = new ResourceBuilder(resourceContext);
        RdfXmlReader reader = new RdfXmlReader().setTripleListener(builder);
        return new XmlMetadataHandler()
                .setHandler(reader.getHandler())
                .setResourceContext(resourceContext)
                .setOutput(new ElasticOut());
    }

    class XmlMetadataHandler extends MetadataHandler {

        private XmlHandler handler;

        private ResourceContext resourceContext;

        private RdfOutput output;

        public XmlMetadataHandler setHandler(XmlHandler handler) {
            this.handler = handler;
            this.handler.setDefaultNamespace("oai_dc","http://www.openarchives.org/OAI/2.0/oai_dc/");
            return this;
        }

        public XmlMetadataHandler setResourceContext(ResourceContext resourceContext) {
            this.resourceContext = resourceContext;
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
                IRI iri = IRI.builder().scheme("http")
                        .host(settings.get("index"))
                        .query(settings.get("type"))
                        .fragment(identifier).build();
                resourceContext.getResource().id(iri);
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
                handler.setDefaultNamespace(settings.get("type"), nsURI);
                resourceContext.getNamespaceContext().addNamespace(settings.get("type"), nsURI);
            }
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

    class ResourceBuilder implements TripleListener {

        ResourceContext resourceContext;

        ResourceBuilder(ResourceContext resourceContext) {
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
        public ResourceBuilder newIdentifier(IRI identifier) {
            resourceContext.setResource(resourceContext.newResource());
            resourceContext.getResource().id(identifier);
            return this;
        }

        @Override
        public ResourceBuilder triple(Triple triple) {
            resourceContext.getResource().add(triple);
            return this;
        }

        @Override
        public TripleListener end() {
            return this;
        }
    }

    class ElasticOut extends RdfOutput {
        @Override
        public RdfOutput output(ResourceContext resourceContext) throws IOException {
            sink.output(resourceContext, resourceContext.getResource(), resourceContext.getContentBuilder());
            return this;
        }
    }

}
