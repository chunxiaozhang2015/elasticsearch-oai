package org.xbib.elasticsearch.oai;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestRequest;
import org.xbib.oai.exceptions.OAIException;
import org.xbib.oai.server.ServerOAIRequest;
import org.xbib.util.DateUtil;
import java.util.Date;

public class OAIServer extends AbstractLifecycleComponent<OAIServer>  {

    private final Logger logger = LogManager.getLogger(OAIServer.class);

    @Inject
    public OAIServer(Settings settings) {
        super(settings);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {

    }

    @Override
    protected void doClose() throws ElasticsearchException {

    }

    public String verbOf(RestRequest request) {
        return request.hasParam("verb") ? request.param("verb") : "identify";
    }

    public void listRecords(final ServerOAIRequest request, SearchRequestBuilder searchRequestBuilder)
            throws OAIException {
        String index = getIndexFrom(request);
        String type = getTypeFrom(request);
        Date dateFrom = request.getFrom();
        Date dateUntil = request.getUntil();
        if (dateFrom == null || dateUntil == null || dateFrom.before(dateUntil)) {
            throw new OAIException("illegal date arguments");
        }
        String from = DateUtil.formatDateISO(dateFrom);
        String to = DateUtil.formatDateISO(dateUntil);
        QueryBuilder query = QueryBuilders
                .rangeQuery("xbib:timestamp")
                .from(from)
                .to(to)
                .includeLower(true)
                .includeUpper(true);
        searchRequestBuilder
                .setIndices(index)
                .setTypes(type)
                .setFrom(request.getResumptionToken().getPosition())
                .setSize(request.getResumptionToken().getInterval())
                .setQuery(query);
    }

    private String getIndexFrom(ServerOAIRequest request) {
        String index = null;
        String path = request.getPath();
        path = path != null && path.startsWith("/oai") ? path.substring(4) : path;
        if (path != null) {
            String[] spec = path.split("/");
            if (spec.length > 1) {
                index = spec[spec.length - 2];
            } else if (spec.length == 1) {
                index = spec[spec.length - 1];
            }
        }
        return index;
    }

    private String getTypeFrom(ServerOAIRequest request) {
        String type = null;
        String path = request.getPath();
        path = path != null && path.startsWith("/oai") ? path.substring(4) : path;
        if (path != null) {
            String[] spec = path.split("/");
            if (spec.length > 1) {
                type = spec[spec.length - 1];
            } else if (spec.length == 1) {
                type = null;
            }
        }
        return type;
    }

}
