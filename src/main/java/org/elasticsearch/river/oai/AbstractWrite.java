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
package org.elasticsearch.river.oai;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import org.xbib.rdf.BlankNode;
import org.xbib.rdf.Literal;
import org.xbib.rdf.Property;
import org.xbib.rdf.Resource;
import org.xbib.xml.NamespaceContext;
import org.xbib.xml.SimpleNamespaceContext;

/**
 * Abstract class for indexing resources to Elasticsearch
 *
 * @author <a href="mailto:joergprante@gmail.com">J&ouml;rg Prante</a>
 */
public abstract class AbstractWrite<S extends Resource<?, ?, ?>, P extends Property, O extends Literal<?>> {

    private static final NamespaceContext context = SimpleNamespaceContext.getInstance();
    private volatile XContentBuilder builder;
    protected String index;
    protected String type;
    protected char delimiter;

    public AbstractWrite(String index, String type) {
        this(index, type, ':');
    }

    public AbstractWrite(String index, String type, char delimiter) {
        this.delimiter = delimiter;
        this.index = index;
        this.type = type;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String createId(Resource resource) {
        if (resource.getIdentifier() == null) {
            return null;
        }
        String id = resource.getIdentifier().getFragment();
        if (id == null) {
            id = resource.getIdentifier().toString();
        }
        return id;
    }

    public abstract void write(Client client, Resource<S, P, O> resource)
            throws IOException;

    public abstract void flush(Client client)
            throws IOException;

    protected void build(Resource<S, P, O> resource) throws IOException {
        this.builder = jsonBuilder();
        try {
            builder.startObject();
            build(builder, resource);
            builder.endObject();
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    protected char getDelimiter() {
        return delimiter;
    }

    protected XContentBuilder getBuilder() {
        return builder;
    }

    protected void build(XContentBuilder builder, Resource<S, P, O> resource)
            throws IOException, URISyntaxException {
        // iterate over properties
        Iterator<P> it = resource.predicateSet(resource.getSubject()).iterator();
        while (it.hasNext()) {
            P predicate = it.next();
            Collection<O> values = resource.objectSet(predicate);
            if (values == null) {
                throw new IllegalArgumentException(
                        "can't build property value set for predicate URI "
                        + predicate);
            }
            // drop values with size 0 silently
            if (values.size() == 1) {
                // single value
                O value = values.iterator().next();
                if (!(value instanceof BlankNode)) {
                    builder.field(
                            context.abbreviate(predicate.getURI()),
                            value.toString());
                }
            } else if (values.size() > 1) {
                // array of values
                Collection<O> properties = filterBlankNodes(values, new ArrayList<O>());
                if (!properties.isEmpty()) {
                    builder.startArray(context.abbreviate(predicate.getURI()));
                    for (O value : properties) {
                        builder.value(value.toString());
                    }
                    builder.endArray();
                }
            }
        }
        // then, iterate over resources
        Map<P, Collection<Resource<S, P, O>>> m = resource.resources();
        Iterator<P> resIt = m.keySet().iterator();
        while (resIt.hasNext()) {
            P predicate = resIt.next();
            Collection<Resource<S, P, O>> resources = m.get(predicate);
            // drop resources with size 0 silently
            if (resources.size() == 1) {
                // single resource
                builder.startObject(context.abbreviate(predicate.getURI()));
                build(builder, resources.iterator().next());
                builder.endObject();
            } else if (resources.size() > 1) {
                // array of resources
                builder.startArray(context.abbreviate(predicate.getURI()));
                for (Resource<S, P, O> child : resources) {
                    builder.startObject();
                    build(builder, child);
                    builder.endObject();
                }
                builder.endArray();
            }
        }
    }

    private Collection<O> filterBlankNodes(Collection<O> objects, Collection<O> properties) {
        for (O object : objects) {
            if (object instanceof BlankNode) {
                continue;
            }
            properties.add(object);
        }
        return properties;
    }
}
