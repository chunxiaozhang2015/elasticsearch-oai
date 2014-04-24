
package org.xbib.elasticsearch.rest.action.river;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import org.xbib.common.xcontent.ToXContent;
import org.xbib.common.xcontent.XContentBuilder;
import org.xbib.elasticsearch.plugin.river.StatefulRiver;
import org.xbib.elasticsearch.rest.action.support.XContentRestResponse;
import org.xbib.elasticsearch.rest.action.support.AbstractRestRiverAction;
import org.xbib.elasticsearch.rest.action.support.XContentThrowableRestResponse;

import java.io.IOException;

import static org.xbib.common.xcontent.XContentFactory.jsonBuilder;

public class RestRiverStateAction extends AbstractRestRiverAction {

    @Inject
    public RestRiverStateAction(Settings settings, Client client,
                                RestController controller,
                                Injector injector) {
        super(settings, client, injector);
        controller.registerHandler(RestRequest.Method.GET, "/_river/_state", this);
        controller.registerHandler(RestRequest.Method.GET, "/_river/{river}/_state", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel) {
        try {
            String riverName = request.param("river");
            XContentBuilder builder = jsonBuilder();
            if (request.paramAsBoolean("pretty", false)) {
                builder.prettyPrint().lfAtEnd();
            }
            builder.startObject().startArray("result");
            rivers(injector).entrySet().stream()
                    .filter(entry -> (riverName == null || entry.getKey().getName().equals(riverName)) && (entry.getValue() instanceof StatefulRiver))
                    .forEach(entry -> {
                StatefulRiver river = (StatefulRiver) entry.getValue();
                try {
                    builder.startObject().field("state");
                    river.getRiverState().toXContent(builder, ToXContent.EMPTY_PARAMS);
                    builder.endObject();
                } catch (IOException e) {
                    // ignore
                }
            });
            builder.endArray().endObject();
            channel.sendResponse(new XContentRestResponse(request, RestStatus.OK, builder));
        } catch (IOException ioe) {
            try {
                channel.sendResponse(new XContentThrowableRestResponse(request, ioe));
            } catch (IOException e) {
                logger.error("unable to send response to client");
            }
        }
    }

}
