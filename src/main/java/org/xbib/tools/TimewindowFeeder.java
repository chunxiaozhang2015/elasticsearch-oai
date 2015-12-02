package org.xbib.tools;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.xbib.elasticsearch.helper.client.LongAdderIngestMetric;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class TimewindowFeeder extends Feeder {

    private final static Logger logger = LogManager.getLogger(TimewindowFeeder.class.getSimpleName());

    protected abstract void setIndex(String index);

    protected abstract String getIndex();

    protected abstract void setConcreteIndex(String concreteIndex);

    protected abstract String getConcreteIndex();

    @Override
    protected void prepareSink() throws IOException {
        if (ingest == null) {
            Integer maxbulkactions = settings.getAsInt("maxbulkactions", 1000);
            Integer maxconcurrentbulkrequests = settings.getAsInt("maxconcurrentbulkrequests",
                    Runtime.getRuntime().availableProcessors());
            ingest = createIngest();
            ingest.maxActionsPerRequest(maxbulkactions)
                    .maxConcurrentRequests(maxconcurrentbulkrequests);
        }
        String timeWindow = settings.get("timewindow") != null ?
                DateTimeFormat.forPattern(settings.get("timewindow")).print(new DateTime()) : "";
        setConcreteIndex(resolveAlias(settings.get("index") + timeWindow));
        Pattern pattern = Pattern.compile("^(.*)\\d+$");
        Matcher m = pattern.matcher(getConcreteIndex());
        setIndex(m.matches() ? m.group(1) : getConcreteIndex());
        logger.info("base index name = {}, concrete index name = {}", getIndex(), getConcreteIndex());
        super.prepareSink();
    }

    @Override
    protected TimewindowFeeder createIndex(String index) throws IOException {
        ingest.init(Settings.settingsBuilder()
                .put("cluster.name", settings.get("elasticsearch.cluster"))
                .put("host", settings.get("elasticsearch.host"))
                .put("port", settings.getAsInt("elasticsearch.port", 9300))
                .put("sniff", settings.getAsBoolean("elasticsearch.sniff", false))
                .put("autodiscover", settings.getAsBoolean("elasticsearch.autodicover", false))
                .build(), new LongAdderIngestMetric());
        if (ingest.client() != null) {
            ingest.waitForCluster(ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30));
            if (settings.getAsBoolean("onlyaliases", false)) {
                updateAliases();
                return this;
            }
            try {
                String indexSettings = settings.get("index-settings",
                        "classpath:org/xbib/tools/feed/elasticsearch/settings.json");
                InputStream indexSettingsInput = (indexSettings.startsWith("classpath:") ?
                        new URL(null, indexSettings, new ClasspathURLStreamHandler()) :
                        new URL(indexSettings)).openStream();
                String indexMappings = settings.get("index-mapping",
                        "classpath:org/xbib/tools/feed/elasticsearch/mapping.json");
                InputStream indexMappingsInput = (indexMappings.startsWith("classpath:") ?
                        new URL(null, indexMappings, new ClasspathURLStreamHandler()) :
                        new URL(indexMappings)).openStream();
                ingest.newIndex(getConcreteIndex(), getType(),
                        indexSettingsInput, indexMappingsInput);
                indexSettingsInput.close();
                indexMappingsInput.close();
                ingest.startBulk(getConcreteIndex(), -1, 1);
            } catch (Exception e) {
                if (!settings.getAsBoolean("ignoreindexcreationerror", false)) {
                    throw e;
                } else {
                    logger.warn("index creation error, but configured to ignore", e);
                }
            }
        }
        return this;
    }

    protected String resolveAlias(String alias) {
        if (ingest.client() == null) {
            return alias;
        }
        GetAliasesRequestBuilder getAliasesRequestBuilder = new GetAliasesRequestBuilder(ingest.client(), GetAliasesAction.INSTANCE);
        GetAliasesResponse getAliasesResponse = getAliasesRequestBuilder.setAliases(alias).execute().actionGet();
        if (!getAliasesResponse.getAliases().isEmpty()) {
            return getAliasesResponse.getAliases().keys().iterator().next().value;
        }
        return alias;
    }

    protected void updateAliases() {
        if (ingest.client() == null) {
            return;
        }
        String index = getIndex();
        String concreteIndex = getConcreteIndex();
        if (!index.equals(concreteIndex)) {
            IndicesAliasesRequestBuilder requestBuilder = new IndicesAliasesRequestBuilder(ingest.client(), IndicesAliasesAction.INSTANCE);
            GetAliasesRequestBuilder getAliasesRequestBuilder = new GetAliasesRequestBuilder(ingest.client(), GetAliasesAction.INSTANCE);
            GetAliasesResponse getAliasesResponse = getAliasesRequestBuilder.setAliases(index).execute().actionGet();
            if (getAliasesResponse.getAliases().isEmpty()) {
                logger.info("adding alias {} to index {}", index, concreteIndex);
                requestBuilder.addAlias(concreteIndex, index);
                // identifier is alias
                if (settings.get("identifier") != null) {
                    requestBuilder.addAlias(concreteIndex, settings.get("identifier"));
                }
            } else {
                for (ObjectCursor<String> indexName : getAliasesResponse.getAliases().keys()) {
                    if (indexName.value.startsWith(index)) {
                        logger.info("switching alias {} from index {} to index {}", index, indexName.value, concreteIndex);
                        requestBuilder.removeAlias(indexName.value, index)
                                .addAlias(concreteIndex, index);
                        if (settings.get("identifier") != null) {
                            requestBuilder.removeAlias(indexName.value, settings.get("identifier"))
                                    .addAlias(concreteIndex, settings.get("identifier"));
                        }
                    }
                }
            }
            requestBuilder.execute().actionGet();
            if (settings.getAsBoolean("retention.enabled", false)) {
                performRetentionPolicy(
                        getIndex(),
                        getConcreteIndex(),
                        settings.getAsInt("retention.diff", 48),
                        settings.getAsInt("retention.mintokeep", 2));
            }
        }
    }

    public void performRetentionPolicy(String index, String concreteIndex, int timestampdiff, int mintokeep) {
        if (ingest.client() == null) {
            return;
        }
        if (index.equals(concreteIndex)) {
            return;
        }
        GetIndexRequestBuilder getIndexRequestBuilder = new GetIndexRequestBuilder(ingest.client(), GetIndexAction.INSTANCE);
        GetIndexResponse getIndexResponse = getIndexRequestBuilder.execute().actionGet();
        Pattern pattern = Pattern.compile("^(.*?)(\\d+)$");
        List<String> indices = new ArrayList<>();
        logger.info("{} indices", getIndexResponse.getIndices().length);
        for (String s : getIndexResponse.getIndices()) {
            Matcher m = pattern.matcher(s);
            if (m.matches()) {
                if (index.equals(m.group(1)) && !s.equals(concreteIndex)) {
                    indices.add(s);
                }
            }
        }
        if (indices.isEmpty()) {
            logger.info("no indices found, retention policy skipped");
            return;
        }
        if (mintokeep > 0 && indices.size() < mintokeep) {
            logger.info("{} indices found, not enough for retention policy ({}),  skipped",
                    indices.size(), mintokeep);
            return;
        } else {
            logger.info("candidates for deletion = {}", indices);
        }
        List<String> indicesToDelete = new ArrayList<String>();
        // our index
        Matcher m1 = pattern.matcher(concreteIndex);
        if (m1.matches()) {
            Integer i1 = Integer.parseInt(m1.group(2));
            for (String s : indices) {
                Matcher m2 = pattern.matcher(s);
                if (m2.matches()) {
                    Integer i2 = Integer.parseInt(m2.group(2));
                    int kept = 1 + indices.size() - indicesToDelete.size();
                    if (timestampdiff > 0 && i1 - i2 > timestampdiff && mintokeep <= kept) {
                        indicesToDelete.add(s);
                    }
                }
            }
        }
        logger.info("indices to delete = {}", indicesToDelete);
        if (indicesToDelete.isEmpty()) {
            logger.info("not enough indices found to delete, retention policy complete");
            return;
        }
        String[] s = indicesToDelete.toArray(new String[indicesToDelete.size()]);
        DeleteIndexRequestBuilder requestBuilder = new DeleteIndexRequestBuilder(ingest.client(), DeleteIndexAction.INSTANCE, s);
        DeleteIndexResponse response = requestBuilder.execute().actionGet();
        if (!response.isAcknowledged()) {
            logger.warn("retention delete index operation was not acknowledged");
        }
    }

}
