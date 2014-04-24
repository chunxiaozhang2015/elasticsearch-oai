
package org.xbib.elasticsearch.rest.action.support;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiversService;

import org.xbib.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;

public abstract class AbstractRestRiverAction extends BaseRestHandler {

    protected Injector injector;

    public AbstractRestRiverAction(Settings settings, Client client, Injector injector) {
        super(settings, client);
        this.injector = injector;
    }

    protected void respond(boolean success, RestRequest request, RestChannel channel, String error, RestStatus status) {
        try {
            XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
            if (request.paramAsBoolean("pretty", false)) {
                builder.prettyPrint();
            }
            builder.startObject()
                 .field("success", success);
            if (error != null) {
                builder.field("error", error);
            }
            builder.endObject();
            channel.sendResponse(new XContentRestResponse(request, status, builder));
        } catch (IOException e) {
            errorResponse(request, channel, e);
        }
    }

    protected void errorResponse(RestRequest request, RestChannel channel, Throwable e) {
        try {
            channel.sendResponse(new XContentThrowableRestResponse(request, e));
        } catch (IOException e1) {
            logger.error("Failed to send failure response", e1);
        }
    }

    /**
     * Retrieve the registered rivers using reflection (UGLY HACK!!)
     * @param injector injector
     * @return map of rivers or null if not possible
     */
    protected ImmutableMap<RiverName, River> rivers(Injector injector) {
        RiversService riversService = injector.getInstance(RiversService.class);
        try {
            Field field = RiversService.class.getDeclaredField("rivers");
            if (field != null) {
                field.setAccessible(true);
                return (ImmutableMap<RiverName, River>) field.get(riversService);
            }
        } catch (Throwable e) {
            //
        }
        return ImmutableMap.copyOf(Collections.emptyMap());
    }
}
