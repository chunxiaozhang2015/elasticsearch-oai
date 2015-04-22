package org.xbib.elasticsearch.plugin.rest.oai;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.support.RestResponseListener;
import org.elasticsearch.search.SearchHit;
import org.xbib.oai.OAIConstants;
import org.xbib.oai.server.ServerOAIRequest;
import org.xbib.xml.XSI;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.util.XMLEventConsumer;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class OAIResponseBuilder extends RestResponseListener<SearchResponse> implements OAIConstants {

    private final static XMLEventFactory eventFactory = XMLEventFactory.newInstance();

    private final static XMLOutputFactory outputfactory  = XMLOutputFactory.newInstance();

    private final static DateTimeFormatter formatter = ISODateTimeFormat.dateTimeNoMillis();

    private final Settings settings;

    private XMLEventConsumer consumer;

    public OAIResponseBuilder(Settings settings, RestChannel channel) {
        super(channel);
        this.settings = settings;
    }

    protected Settings settings() {
        return settings;
    }

    @Override
    public RestResponse buildResponse(SearchResponse response) throws Exception {
        return buildResponse(response, channel.bytesOutput());
    }

    protected RestResponse buildResponse(SearchResponse response, BytesStreamOutput out) throws Exception {
        if (response.getHits().getHits().length > 0) {
            for (SearchHit hits : response.getHits()) {
                // TODO
            }
        }
        return new BytesRestResponse(RestStatus.OK, "text/xml; charset=UTF-8", out.bytes(), true);
    }

    protected OAIResponseBuilder setOutputStream(OutputStream out) throws XMLStreamException {
        this.consumer = outputfactory.createXMLEventWriter(out);
        return this;
    }

    protected OAIResponseBuilder beginDocument() throws XMLStreamException {
        consumer.add(eventFactory.createStartDocument());
        return this;
    }

    protected OAIResponseBuilder endDocument() throws XMLStreamException, IOException {
        consumer.add(eventFactory.createEndDocument());
        return this;
    }

    protected OAIResponseBuilder beginElement(String name) throws XMLStreamException {
        consumer.add(eventFactory.createStartElement(toQName(NS_URI, name), null, null));
        return this;
    }

    protected OAIResponseBuilder endElement(String name) throws XMLStreamException {
        consumer.add(eventFactory.createEndElement(toQName(NS_URI, name), null));
        return this;
    }

    protected OAIResponseBuilder element(String name) throws XMLStreamException {
        consumer.add(eventFactory.createStartElement(toQName(NS_URI, name), null, null));
        consumer.add(eventFactory.createEndElement(toQName(NS_URI, name), null));
        return this;
    }

    protected OAIResponseBuilder element(String name, String value) throws XMLStreamException {
        consumer.add(eventFactory.createStartElement(toQName(NS_URI, name), null, null));
        consumer.add(eventFactory.createCharacters(value));
        consumer.add(eventFactory.createEndElement(toQName(NS_URI, name), null));
        return this;
    }

    protected OAIResponseBuilder element(String name, Date value) throws XMLStreamException {
        consumer.add(eventFactory.createStartElement(toQName(NS_URI, name), null, null));
        consumer.add(eventFactory.createCharacters(formatter.print(value.getTime())));
        consumer.add(eventFactory.createEndElement(toQName(NS_URI, name), null));
        return this;
    }

    protected OAIResponseBuilder beginResponse(String baseURL, ServerOAIRequest serverOAIRequest) throws XMLStreamException {
        consumer.add(eventFactory.createStartElement(toQName(NS_URI, "OAI-PMH"), null, null));
        consumer.add(eventFactory.createNamespace(NS_URI));
        consumer.add(eventFactory.createNamespace(XSI.NS_PREFIX, XSI.NS_URI));
        consumer.add(eventFactory.createAttribute(XSI.NS_PREFIX, XSI.NS_URI,
                "schemaLocation", "http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd"));
        element("responseDate", new Date());
        request(serverOAIRequest.getParameterMap(), baseURL);
        return this;
    }

    protected OAIResponseBuilder endResponse() throws XMLStreamException {
        consumer.add(eventFactory.createEndElement(toQName(NS_URI, "OAI-PMH"), null));
        return this;
    }

    protected OAIResponseBuilder request(Map<String,Object> attrs, String baseURL) throws XMLStreamException {
        consumer.add(eventFactory.createStartElement(toQName(NS_URI, REQUEST), null, null));
        for (Map.Entry<String,Object> me : attrs.entrySet()) {
            Object o = me.getValue();
            if (!(o instanceof List)) {
                o = Arrays.asList(o);
            }
            for (Object value : (List)o) {
                consumer.add(eventFactory.createAttribute(me.getKey(),value.toString()));
            }
        }
        consumer.add(eventFactory.createCharacters(baseURL));
        consumer.add(eventFactory.createEndElement(toQName(NS_URI, REQUEST), null));
        return this;
    }

    protected QName toQName(String namespaceUri, String qname) {
        int i = qname.indexOf(':');
        if (i == -1) {
            return new QName(namespaceUri, qname);
        } else {
            String prefix = qname.substring(0, i);
            String localPart = qname.substring(i + 1);
            return new QName(namespaceUri, localPart, prefix);
        }
    }
}
