package org.xbib.elasticsearch.plugin.river.oai;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.loader.JsonSettingsLoader;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.xbib.elasticsearch.action.river.execute.RunnableRiver;
import org.xbib.elasticsearch.action.river.state.RiverState;
import org.xbib.elasticsearch.action.river.state.StatefulRiver;
import org.xbib.elasticsearch.plugin.feeder.Feeder;
import org.xbib.elasticsearch.plugin.feeder.oai.OAIFeeder;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Open Archive Initiative river
 * The river is a thin wrapper around the OAI feeder.
 */
public class OAIRiver extends AbstractRiverComponent implements RunnableRiver, StatefulRiver {

    private volatile Thread riverThread;

    private final Feeder feeder;

    private final Client client;

    private volatile boolean closed;

    @Inject
    public OAIRiver(RiverName riverName, RiverSettings riverSettings,
                    @RiverIndexName String riverIndexName, Client client) {
        super(riverName, riverSettings);
        if (!riverSettings.settings().containsKey("oai")) {
            throw new IllegalArgumentException("no 'oai' settings in river settings?");
        }
        this.client = client;
        this.feeder = createFeeder(riverSettings);
    }

    private Feeder createFeeder(RiverSettings riverSettings) {
        Feeder feeder = null;
        try {
            Map<String, Object> spec = (Map<String, Object>) riverSettings.settings().get("oai");
            Map<String, String> loadedSettings = new JsonSettingsLoader().load(jsonBuilder().map(spec).string());
            Settings mySettings = settingsBuilder().put(loadedSettings).build();
            feeder = new OAIFeeder();
            feeder.setType("oai").setSpec(spec).setSettings(mySettings);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return feeder;
    }

    @Override
    public void start() {
        feeder.setClient(client);
        feeder.setRiverState(new RiverState()
                        .setEnabled(true)
                        .setStarted(new Date())
                        .setName(riverName.getName())
                        .setType(riverName.getType())
                        .setCoordinates("_river", riverName.getName(), "_custom")
        );
        this.riverThread = EsExecutors.daemonThreadFactory(settings.globalSettings(),
                "river(" + riverName().getType() + "/" + riverName().getName() + ")")
                .newThread(feeder);
        feeder.schedule(riverThread);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        logger.info("closing river({}/{})", riverName.getType(), riverName.getName());
        try {
            feeder.getRiverState().setEnabled(false).save(client);
            feeder.shutdown();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        if (riverThread != null) {
            riverThread.interrupt();
        }
    }

    @Override
    public RiverState getRiverState() {
        return feeder.getRiverState();
    }

    @Override
    public void run() {
        feeder.run();
    }

}
