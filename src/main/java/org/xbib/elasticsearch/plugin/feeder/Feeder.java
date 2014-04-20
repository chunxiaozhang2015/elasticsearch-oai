
package org.xbib.elasticsearch.plugin.feeder;

import org.elasticsearch.client.Client;
import org.xbib.common.settings.Settings;
import org.xbib.elasticsearch.sink.ResourceSink;
import org.xbib.elasticsearch.support.client.Ingest;
import org.xbib.elasticsearch.support.client.bulk.BulkTransportClient;
import org.xbib.elasticsearch.support.client.ingest.IngestTransportClient;
import org.xbib.elasticsearch.support.client.node.NodeClient;
import org.xbib.logging.Logger;
import org.xbib.logging.LoggerFactory;
import org.xbib.metric.MeterMetric;
import org.xbib.pipeline.AbstractPipeline;
import org.xbib.pipeline.Pipeline;
import org.xbib.pipeline.PipelineException;
import org.xbib.pipeline.PipelineProvider;
import org.xbib.pipeline.PipelineRequest;
import org.xbib.pipeline.element.URIPipelineElement;
import org.xbib.pipeline.simple.MetricSimplePipelineExecutor;

import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.xbib.common.settings.ImmutableSettings.settingsBuilder;

public abstract class Feeder<T, R extends PipelineRequest, P extends Pipeline<T,R>>
        extends AbstractPipeline<URIPipelineElement, PipelineException> implements Tool {

    private final static Logger logger = LoggerFactory.getLogger(Feeder.class.getSimpleName());

    private final static URIPipelineElement uriPipelineElement = new URIPipelineElement();

    protected static Settings settings;

    protected static Queue<URI> input;

    protected MetricSimplePipelineExecutor<T,R,P> executor;

    protected static Ingest client;

    protected static ResourceSink sink;

    private boolean done = false;

    public Feeder<T,R,P> settings(Settings newSettings) {
        settings = newSettings;
        return this;
    }

    public void setClient(Client c) {
        Integer maxbulkactions = settings.getAsInt("maxbulkactions", 1000);
        Integer maxconcurrentbulkrequests = settings.getAsInt("maxconcurrentbulkrequests",
                Runtime.getRuntime().availableProcessors());
        client = new NodeClient()
                .maxActionsPerBulkRequest(maxbulkactions)
                .maxConcurrentBulkRequests(maxconcurrentbulkrequests)
                .newClient(c);
    }

    @Override
    public Feeder<T,R,P> readFrom(Reader reader) {
        settings = settingsBuilder().loadFromReader(reader).build();
        return this;
    }

    @Override
    public void run() {
        try {
            logger.info("preparing run with settings {}", settings.getAsMap());
            prepare();
            logger.info("executing");
            executor = new MetricSimplePipelineExecutor<T, R, P>()
                    .setConcurrency(settings.getAsInt("concurrency", 1))
                    .setPipelineProvider(pipelineProvider())
                    .prepare()
                    .execute()
                    .waitFor();
            logger.info("execution completed");
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        } finally {
            try {
                cleanup();
                if (executor != null) {
                    executor.shutdown();
                }
            } catch (InterruptedException e) {
                logger.warn("interrupted", e);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
            logger.info("run completed");
        }
    }

    protected Feeder<T,R,P> prepare() throws IOException {
        Integer maxbulkactions = settings.getAsInt("maxbulkactions", 1000);
        Integer maxconcurrentbulkrequests = settings.getAsInt("maxconcurrentbulkrequests",
                Runtime.getRuntime().availableProcessors());
        if (client == null) {
            client = "ingest".equals(settings.get("client")) ?
                      new IngestTransportClient()
                    : new BulkTransportClient();
            client.maxActionsPerBulkRequest(maxbulkactions)
                    .maxConcurrentBulkRequests(maxconcurrentbulkrequests);
            client.newClient(URI.create(settings.get("elasticsearch")));
        }
        String index = settings.get("index");
        String type = settings.get("type");
        try {
            client.setting(Feeder.class.getResourceAsStream(index + ".json"));
        } catch (Exception e) {
            // ignore
        }
        try {
            client.mapping(type, Feeder.class.getResourceAsStream(index + "/" + type + ".json"));
        } catch (Exception e) {
            // ignore
        }
        client.setIndex(index)
                .setType(type)
                .shards(settings.getAsInt("shards", 1))
                .replica(settings.getAsInt("replica", 0))
                .newIndex()
                .startBulk();
        sink = new ResourceSink(client);
        input = new ConcurrentLinkedQueue<URI>();
        String[] inputs = settings.getAsArray("input");
        for (String uri : inputs) {
            input.offer(URI.create(uri));
        }
        logger.info("input = {}", input);
        return this;
    }

    protected Feeder<T,R,P> cleanup() throws IOException {
        client.flush().stopBulk();
        // set to requested replica level
        client.updateReplicaLevel(settings.getAsInt("replica", 0));
        return this;
    }

    @Override
    public void close() throws IOException {
        logger.info("close (no op)");
    }

    @Override
    public boolean hasNext() {
        return !done && !input.isEmpty();
    }

    @Override
    public URIPipelineElement next() {
        URI uri = input.poll();
        done = uri == null;
        uriPipelineElement.set(uri);
        if (done) {
            return uriPipelineElement;
        }
        return uriPipelineElement;
    }

    @Override
    public void newRequest(Pipeline<MeterMetric, URIPipelineElement> pipeline, URIPipelineElement request) {
        try {
            process(request.get());
        } catch (Exception ex) {
            logger.error("error while getting next input: " + ex.getMessage(), ex);
        }
    }

    @Override
    public void error(Pipeline<MeterMetric, URIPipelineElement> pipeline, URIPipelineElement request, PipelineException error) {
        logger.error(error.getMessage(), error);
    }

    protected abstract PipelineProvider<P> pipelineProvider();

    protected abstract void process(URI uri) throws Exception;

}
