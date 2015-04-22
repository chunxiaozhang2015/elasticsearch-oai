package org.xbib.elasticsearch.plugin.rest.oai;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.xbib.elasticsearch.oai.OAIServer;
import org.xbib.oai.OAIDateResolution;
import org.xbib.oai.exceptions.OAIException;
import org.xbib.oai.server.listrecords.ListRecordsServerRequest;
import org.xbib.oai.util.ResumptionToken;
import org.xbib.util.DateUtil;

import java.util.Date;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestOAISearchAction extends BaseRestHandler {

    private final OAIServer oaiServer;

    @Inject
    public RestOAISearchAction(Settings settings, Client client, RestController controller,
                               OAIServer oaiServer) {
        super(settings, controller, client);
        this.oaiServer = oaiServer;
        controller.registerHandler(GET, "/_oai", this);
        controller.registerHandler(POST, "/_oai", this);
        controller.registerHandler(GET, "/{index}/_oai", this);
        controller.registerHandler(POST, "/{index}/_oai", this);
        controller.registerHandler(GET, "/{index}/{type}/_oai", this);
        controller.registerHandler(POST, "/{index}/{type}/_oai", this);
        controller.registerHandler(GET, "/_oai/template", this);
        controller.registerHandler(POST, "/_search_arrayformat/template", this);
        controller.registerHandler(GET, "/{index}/_search_arrayformat/template", this);
        controller.registerHandler(POST, "/{index}/_search_arrayformat/template", this);
        controller.registerHandler(GET, "/{index}/{type}/_search_arrayformat/template", this);
        controller.registerHandler(POST, "/{index}/{type}/_search_arrayformat/template", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        String verb = oaiServer.verbOf(request);
        OAIResponseBuilder builder = new OAIResponseBuilder(settings, channel);
        try {
            switch (verb) {
                case "ListRecords": {
                    SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client);
                    oaiServer.listRecords(makeListRecordsRequest(request), searchRequestBuilder);
                    client.search(searchRequestBuilder.request(), builder);
                    break;
                }
                default: {
                    builder.onFailure(new ElasticsearchIllegalStateException("unknown verb: " + verb));
                    break;
                }
            }
        } catch (OAIException e) {
            builder.onFailure(e);
        }
    }

    private ListRecordsServerRequest makeListRecordsRequest(RestRequest restRequest) {
        Date from = DateUtil.parseDateISO(restRequest.param("from"));
        Date until = DateUtil.parseDateISO(restRequest.param("until"));
        String set = restRequest.param("set");
        String metadataPrefix = restRequest.param("metadataPrefix");
        ResumptionToken token = ResumptionToken.newToken(restRequest.param("resumptionToken"));
        return new ListRecordsServerRequest()
                .setResumptionToken(token)
                .setFrom(from, OAIDateResolution.SECOND)
                .setUntil(until, OAIDateResolution.SECOND)
                .setSet(set)
                .setMetadataPrefix(metadataPrefix);
    }

}
