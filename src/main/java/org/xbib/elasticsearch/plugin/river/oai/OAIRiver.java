
package org.xbib.elasticsearch.plugin.river.oai;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import org.xbib.common.settings.Settings;
import org.xbib.elasticsearch.plugin.feeder.oai.OAIFeeder;

import java.io.IOException;
import java.util.Map;

import static org.xbib.common.settings.ImmutableSettings.settingsBuilder;

public class OAIRiver extends AbstractRiverComponent implements River {

    private volatile Thread harvesterThread;

    private final OAIFeeder feeder;

    private final Client client;

    @Inject
    public OAIRiver(RiverName riverName, RiverSettings riverSettings,
                    @RiverIndexName String riverIndexName, Client client) {
        super(riverName, riverSettings);
        this.client = client;
        if (!riverSettings.settings().containsKey("oai")) {
            throw new IllegalArgumentException("no 'oai' settings in river settings?");
        }
        Map<String, Object> oaiSettings = (Map<String, Object>) settings.settings().get("oai");

        Settings settings = settingsBuilder()
                .putArray("input", XContentMapValues.extractRawValues("input", oaiSettings))
                .put("concurrency", XContentMapValues.nodeIntegerValue(oaiSettings.get("concurrency"), 1))
                .put("handler", XContentMapValues.nodeStringValue(oaiSettings.get("handler"), "xml"))
                .put("index", XContentMapValues.nodeStringValue(oaiSettings.get("index"), "oai"))
                .put("type", XContentMapValues.nodeStringValue(oaiSettings.get("type"), "oai"))
                .put("shards", XContentMapValues.nodeIntegerValue(oaiSettings.get("shards"), 1))
                .put("replica", XContentMapValues.nodeIntegerValue(oaiSettings.get("replica"), 0))
                .put("maxbulkactions", XContentMapValues.nodeIntegerValue(oaiSettings.get("maxbulkactions"), 1000))
                .put("maxconcurrentbulkrequests", XContentMapValues.nodeIntegerValue(oaiSettings.get("maxconcurrentbulkrequests"), 10))
                .put("trace", XContentMapValues.nodeBooleanValue(oaiSettings.get("trace"), false))
                .put("scrubxml", XContentMapValues.nodeBooleanValue(oaiSettings.get("scrubxml"), true))
                .build();

        feeder = new OAIFeeder();
        feeder.settings(settings);
    }

    @Override
    public void start() {
        feeder.setClient(client);
        harvesterThread = EsExecutors.daemonThreadFactory(settings.globalSettings(),
                "oai_river(" + riverName().name() + ")").newThread(feeder);
        harvesterThread.start();
    }

    @Override
    public void close() {
        logger.info("closing oai_river({})", riverName.name());
        try {
            feeder.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        harvesterThread.interrupt();
    }

}
