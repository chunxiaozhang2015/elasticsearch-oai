package org.xbib.tools;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.support.client.Ingest;
import org.xbib.elasticsearch.support.client.bulk.BulkTransportClient;
import org.xbib.elasticsearch.support.client.ingest.IngestTransportClient;
import org.xbib.elasticsearch.support.client.ingest.MockIngestTransportClient;
import org.xbib.pipeline.Pipeline;
import org.xbib.pipeline.PipelineRequest;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

public abstract class Feeder<T, R extends PipelineRequest, P extends Pipeline<T, R>>
        extends Converter<T, R, P> {

    private final static Logger logger = LogManager.getLogger(Feeder.class.getSimpleName());

    protected static Ingest ingest;

    @Override
    public Feeder<T, R, P> reader(Reader reader) {
        super.reader(reader);
        return this;
    }

    @Override
    public Feeder<T, R, P> writer(Writer writer) {
        super.writer(writer);
        return this;
    }

    protected String getIndex() {
        return settings.get("index");
    }

    protected String getType() {
        return settings.get("type");
    }

    protected Ingest createIngest() {
        return settings.getAsBoolean("mock", false) ?
                new MockIngestTransportClient() :
                "ingest".equals(settings.get("client")) ? new IngestTransportClient() :
                        new BulkTransportClient();
    }

    @Override
    protected Feeder<T, R, P> prepare() throws IOException {
        super.prepare();
        if (ingest == null) {
            Integer maxbulkactions = settings.getAsInt("maxbulkactions", 1000);
            Integer maxconcurrentbulkrequests = settings.getAsInt("maxconcurrentbulkrequests",
                    Runtime.getRuntime().availableProcessors());
            ingest = createIngest();
            ingest.maxActionsPerBulkRequest(maxbulkactions)
                    .maxConcurrentBulkRequests(maxconcurrentbulkrequests);
        }
        createIndex(getIndex());
        return this;
    }

    @Override
    protected Feeder<T, R, P> cleanup() throws IOException {
        super.cleanup();
        if (ingest != null) {
            try {
                logger.info("flush");
                ingest.flushIngest();
                logger.info("waiting for all responses");
                ingest.waitForResponses(TimeValue.timeValueSeconds(120));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error(e.getMessage(), e);
            }
            logger.info("shutdown");
            ingest.shutdown();
        }
        logger.info("done with run");
        return this;
    }

    protected Feeder createIndex(String index) throws IOException {
        ingest.newClient(ImmutableSettings.settingsBuilder()
                .put("cluster.name", settings.get("elasticsearch.cluster"))
                .put("host", settings.get("elasticsearch.host"))
                .put("port", settings.getAsInt("elasticsearch.port", 9300))
                .put("sniff", settings.getAsBoolean("elasticsearch.sniff", false))
                .build());
        ingest.waitForCluster(ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30));
        try {
            beforeIndexCreation(ingest);
            ingest.newIndex(index);
        } catch (Exception e) {
            if (!settings.getAsBoolean("ignoreindexcreationerror", false)) {
                throw e;
            } else {
                logger.warn("index creation error, but configured to ignore");
            }
        }
        return this;
    }

    protected Feeder beforeIndexCreation(Ingest output) throws IOException {
        return this;
    }
}
