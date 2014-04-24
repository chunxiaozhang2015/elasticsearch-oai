
package org.xbib.elasticsearch.rest.action.support;

import org.elasticsearch.rest.RestRequest;

import org.xbib.common.xcontent.XContentBuilder;
import org.xbib.common.xcontent.XContentFactory;
import org.xbib.common.xcontent.XContentType;
import org.xbib.io.FastByteArrayOutputStream;

import java.io.IOException;

public class RestXContentBuilder {

    public static XContentBuilder restContentBuilder(RestRequest request) throws IOException {
        XContentType contentType = XContentType.fromRestContentType(request.param("format", request.header("Content-Type")));
        if (contentType == null) {
            // default to JSON
            contentType = XContentType.JSON;
        }
        XContentBuilder builder = new XContentBuilder(XContentFactory.xContent(contentType),
                new FastByteArrayOutputStream());
        if (request.paramAsBoolean("pretty", false)) {
            builder.prettyPrint().lfAtEnd();
        }
        String casing = request.param("case");
        if (casing != null && "camelCase".equals(casing)) {
            builder.fieldCaseConversion(XContentBuilder.FieldCaseConversion.CAMELCASE);
        } else {
            // we expect all REST interfaces to write results in underscore casing, so
            // no need for double casing
            builder.fieldCaseConversion(XContentBuilder.FieldCaseConversion.NONE);
        }
        return builder;
    }

    public static XContentBuilder emptyBuilder(RestRequest request) throws IOException {
        return restContentBuilder(request).startObject().endObject();
    }

}
