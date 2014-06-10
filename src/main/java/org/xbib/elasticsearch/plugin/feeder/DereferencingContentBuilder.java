package org.xbib.elasticsearch.plugin.feeder;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.get.GetField;
import org.xbib.common.xcontent.XContentBuilder;
import org.xbib.rdf.Identifier;
import org.xbib.rdf.Node;
import org.xbib.rdf.Property;
import org.xbib.rdf.Resource;
import org.xbib.rdf.content.DefaultContentBuilder;
import org.xbib.rdf.context.IRINamespaceContext;
import org.xbib.rdf.context.ResourceContext;

import java.io.IOException;

public class DereferencingContentBuilder<C extends ResourceContext<R>, R extends Resource>
        extends DefaultContentBuilder<C, R> {

    private final IRINamespaceContext namespaceContext;

    private final Client client;

    private final String index;

    private final String type;

    private final String prefix;

    private final String[] field;

    public DereferencingContentBuilder(IRINamespaceContext namespaceContext, Client client, String index, String type, String prefix, String[] field) {
        this.namespaceContext = namespaceContext;
        this.client = client;
        this.index = index;
        this.type = type;
        this.prefix = prefix;
        this.field = field;
    }

    @Override
    protected <S extends Identifier, P extends Property, O extends Node> void expandField(XContentBuilder builder, ResourceContext<R> context, S subject, P predicate, O object) throws IOException {
        if (subject == null || predicate == null || object == null || prefix == null || index == null || type == null || field == null) {
            return;
        }
        String id = object.nativeValue().toString();
        if (!id.startsWith(prefix)) {
            return;
        }
        if (id.equals(subject.id().toString())) {
            return;
        }
        if ("iri".equals(namespaceContext.compact(predicate.id()))) {
            return;
        }
        GetResponse response = client.prepareGet()
                .setIndex(index)
                .setType(type)
                .setId(id)
                .setFields(field)
                .get(TimeValue.timeValueSeconds(5));
        if (response.isExists()) {
            for (String f : field) {
                GetField getField = response.getField(f);
                if (getField != null) {
                    if ("location".equals(f)) {
                        builder.startArray(f)
                                .value(getField.getValues().get(0))
                                .value(getField.getValues().get(1))
                                .endArray();
                    } else {
                        for (Object value : getField.getValues()) {
                            builder.startArray(namespaceContext.compact(predicate.id()))
                                    .value(id).value(value).endArray();
                        }
                    }
                }
            }
        }
    }

    @Override
    protected <S extends Identifier, P extends Property, O extends Node> void expandValue(XContentBuilder builder, ResourceContext<R> context, S subject, P predicate, O object) throws IOException {
        if (subject == null || predicate == null || object == null || prefix == null || index == null || type == null || field == null) {
            return;
        }
        final String id = object.nativeValue().toString();
        if (!id.startsWith(prefix)) {
            return;
        }
        if (id.equals(subject.id().toString())) {
            return;
        }
        if ("iri".equals(namespaceContext.compact(predicate.id()))) {
            return;
        }
        GetResponse response = client.prepareGet()
                .setIndex(index)
                .setType(type)
                .setId(id)
                .setFields(field)
                .get(TimeValue.timeValueSeconds(5));
        if (response.isExists()) {
            for (String f : field) {
                GetField getField = response.getField(f);
                if (getField != null) {
                    for (Object value : getField.getValues()) {
                        builder.value(value);
                    }
                }
            }
        }
    }

}
