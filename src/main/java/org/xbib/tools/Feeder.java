
package org.xbib.tools;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.helper.client.Ingest;
import org.xbib.elasticsearch.helper.client.LongAdderIngestMetric;
import org.xbib.elasticsearch.helper.client.ingest.IngestTransportClient;
import org.xbib.elasticsearch.helper.client.mock.MockTransportClient;
import org.xbib.elasticsearch.helper.client.transport.BulkTransportClient;
import org.xbib.metric.MeterMetric;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.URL;
import java.util.concurrent.ExecutionException;

public abstract class Feeder extends Converter {

    private final static Logger logger = LogManager.getLogger(Feeder.class);

    protected static Ingest ingest;

    @Override
    public Feeder reader(Reader reader) {
        super.reader(reader);
        return this;
    }

    @Override
    public Feeder writer(Writer writer) {
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
                new MockTransportClient() :
                "ingest".equals(settings.get("client")) ? new IngestTransportClient() :
                        new BulkTransportClient();
    }

    @Override
    protected void prepareSink() throws IOException {
        logger.info("preparing ingest");
        if (ingest == null) {
            Integer maxbulkactions = settings.getAsInt("maxbulkactions", 1000);
            Integer maxconcurrentbulkrequests = settings.getAsInt("maxconcurrentbulkrequests",
                    Runtime.getRuntime().availableProcessors());
            ingest = createIngest();
            ingest.maxActionsPerRequest(maxbulkactions)
                    .maxConcurrentRequests(maxconcurrentbulkrequests);
            ingest.init(Settings.settingsBuilder()
                    .put("cluster.name", settings.get("elasticsearch.cluster", "elasticsearch"))
                    .put("host", settings.get("elasticsearch.host", "localhost"))
                    .put("port", settings.getAsInt("elasticsearch.port", 9300))
                    .put("sniff", settings.getAsBoolean("elasticsearch.sniff", false))
                    .put("autodiscover", settings.getAsBoolean("elasticsearch.autodiscover", false))
                    .build(), new LongAdderIngestMetric());
        }
        createIndex(getIndex());
    }

    @Override
    protected Feeder cleanup() throws IOException {
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
            } catch (ExecutionException e) {
                logger.error(e.getMessage(), e);
            }
            logger.info("shutdown");
            ingest.shutdown();
        }
        logger.info("done with run");
        return this;
    }

    @Override
    protected void writeMetrics(MeterMetric metric, Writer writer) throws Exception {
        // TODO
    }

    protected Feeder createIndex(String index) throws IOException {
        if (ingest == null) {
            return this;
        }
        if (settings.get("elasticsearch.cluster") != null) {
            Settings clientSettings = Settings.settingsBuilder()
                    .put("cluster.name", settings.get("elasticsearch.cluster"))
                    .put("host", settings.get("elasticsearch.host"))
                    .put("port", settings.getAsInt("elasticsearch.port", 9300))
                    .put("sniff", settings.getAsBoolean("elasticsearch.sniff", false))
                    .put("autodiscover", settings.getAsBoolean("elasticsearch.autodiscover", false))
                    .build();
            ingest.init(clientSettings, new LongAdderIngestMetric());
        }
        ingest.waitForCluster(ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30));
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
            ingest.newIndex(getIndex(), getType(),
                    indexSettingsInput, indexMappingsInput);
            beforeIndexCreation(ingest);
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
