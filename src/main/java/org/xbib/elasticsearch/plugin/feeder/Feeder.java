
package org.xbib.elasticsearch.plugin.feeder;

import org.elasticsearch.client.Client;

import org.xbib.common.settings.Settings;
import org.xbib.common.unit.TimeValue;
import org.xbib.elasticsearch.plugin.cron.CronExpression;
import org.xbib.elasticsearch.plugin.cron.CronThreadPoolExecutor;
import org.xbib.elasticsearch.plugin.river.RiverState;
import org.xbib.elasticsearch.rdf.ResourceSink;
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
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.collect.Lists.newLinkedList;
import static org.xbib.common.settings.ImmutableSettings.settingsBuilder;

/**
 * Base class for all feeders and feed pipelines.
 *
 * A feed pipeline is a sequence of URI in form of pipeline elements that can be consumed
 * in serial or parallel manner.
 *
 * Variables - mostly read-only - that are common to all pipeline executions are declared
 * as static variables.
 *
 * @param <T>
 * @param <R>
 * @param <P>
 */
public abstract class Feeder<T, R extends PipelineRequest, P extends Pipeline<T,R>>
        extends AbstractPipeline<URIPipelineElement, PipelineException> implements Tool {

    private final static Logger logger = LoggerFactory.getLogger(Feeder.class.getSimpleName());

    /**
     * The settings for the feeder
     */
    protected Settings settings;

    /**
     * The URI sources for the feed processing
     */
    protected Queue<URI> input;

    /**
     * The ingester routine, shared by all pipelines.
     */
    protected Ingest ingest;

    /**
     * Where generated RDF should be sent to.
     */
    protected ResourceSink sink;

    /**
     * This executor can start the feeder at scheduled times.
     */
    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    /**
     * The executor for pipelines.
     */
    private MetricSimplePipelineExecutor<T,R,P> executor;

    /**
     * The state of the feeder, relevant for persistency if the feeder is running in a river
     */
    private RiverState riverState;

    /**
     *  A counter for the state bookkeeping.
     */
    private final AtomicLong counter = new AtomicLong();

    /**
     * A flag that indicates the feeder should terminate.
     */
    private volatile boolean interrupted;

    private List<Feeder> registry = newLinkedList();

    public Feeder() {
        registry.add(this);
    }

    public Feeder(Feeder feeder) {
        this.settings = feeder.settings;
        this.input = feeder.input;
        this.ingest = feeder.ingest;
        this.sink = feeder.sink;
        this.scheduledThreadPoolExecutor = feeder.scheduledThreadPoolExecutor;
        this.riverState = feeder.riverState;
        feeder.registry.add(this);
    }

    @Override
    public void schedule(Thread thread) {
        String[] schedule = settings.getAsArray("schedule");
        Long seconds = settings.getAsTime("interval", TimeValue.timeValueSeconds(0)).seconds();
        if (schedule != null && schedule.length > 0) {
            CronThreadPoolExecutor e = new CronThreadPoolExecutor(settings.getAsInt("cronpoolsize", 4));
            for (String cron : schedule) {
                e.schedule(thread, new CronExpression(cron));
            }
            this.scheduledThreadPoolExecutor = e;
            logger.info("scheduled with cron expressions {}", Arrays.asList(schedule));
        } else if (seconds > 0L) {
            this.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(settings.getAsInt("cronpoolsize", 4));
            scheduledThreadPoolExecutor.scheduleAtFixedRate(thread, 0L, seconds, TimeUnit.SECONDS);
            logger.info("scheduled at fixed rate of {} seconds", seconds);
        } else {
            thread.start();
            logger.info("started");
        }
    }

    public Feeder<T,R,P> setSettings(Settings newSettings) {
        this.settings = newSettings;
        return this;
    }

    public void setClient(Client c) {
        Integer maxbulkactions = settings.getAsInt("maxbulkactions", 1000);
        Integer maxconcurrentbulkrequests = settings.getAsInt("maxconcurrentbulkrequests",
                Runtime.getRuntime().availableProcessors());
        this.ingest = new NodeClient()
                .maxActionsPerBulkRequest(maxbulkactions)
                .maxConcurrentBulkRequests(maxconcurrentbulkrequests)
                .newClient(c);
    }

    public Feeder<T,R,P> setRiverState(RiverState riverState) {
        this.riverState = riverState;
        return this;
    }

    public RiverState getRiverState() {
        return riverState;
    }

    @Override
    public Feeder<T,R,P> readFrom(Reader reader) {
        this.settings = settingsBuilder().loadFromReader(reader).build();
        return this;
    }

    @Override
    public void run() {
        try {
            logger.info("starting run with settings {}", settings.getAsMap());
            prepare();
            this.executor = new MetricSimplePipelineExecutor<T, R, P>()
                    .setConcurrency(settings.getAsInt("concurrency", 1))
                    .setPipelineProvider(pipelineProvider())
                    .prepare()
                    .execute()
                    .waitFor();
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        } finally {

            try {
                if (executor != null) {
                    executor.shutdown();
                    executor = null;
                }
                cleanup();
            } catch (InterruptedException e) {
                logger.warn("interrupted", e);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
            logger.info("run completed with settings {}", settings.getAsMap());
        }
    }

    protected Feeder<T,R,P> prepare() throws IOException {
        Integer maxbulkactions = settings.getAsInt("maxbulkactions", 1000);
        Integer maxconcurrentbulkrequests = settings.getAsInt("maxconcurrentbulkrequests",
                Runtime.getRuntime().availableProcessors());
        if (ingest == null) {
            ingest = "ingest".equals(settings.get("client")) ?
                      new IngestTransportClient()
                    : new BulkTransportClient();
            ingest.maxActionsPerBulkRequest(maxbulkactions)
                    .maxConcurrentBulkRequests(maxconcurrentbulkrequests);
            ingest.newClient(URI.create(settings.get("elasticsearch")));
        }
        String index = settings.get("index");
        String type = settings.get("type");
        try {
            ingest.setting(Feeder.class.getResourceAsStream("/" + index + "/settings"));
        } catch (Exception e) {
            // ignore
        }
        try {
            ingest.mapping(type, Feeder.class.getResourceAsStream("/" + index + "/" + type + ".mapping"));
        } catch (Exception e) {
            // ignore
        }
        ingest.setIndex(index)
                .setType(type)
                .newIndex()
                .startBulk();
        this.sink = new ResourceSink(ingest);
        this.input = new ConcurrentLinkedQueue<URI>();
        String[] inputs = settings.getAsArray("input");
        for (String uri : inputs) {
            input.offer(URI.create(uri));
        }
        logger.info("input = {}", input);
        return this;
    }

    protected Feeder<T,R,P> cleanup() throws IOException {
        ingest.flush().stopBulk();
        ingest.updateReplicaLevel(ingest.settings().getAsInt("index.number_of_replica", 0));
        if (riverState != null) {
            riverState.setActive(false).save(ingest.client());
        }
        return this;
    }

    /**
     * Interrupt this feeder. The feeder pipelines will terminate as soon as the next cycle is
     * executed.
     * @param state true if the feeder should be interrupted
     */
    public void setInterrupted(boolean state) {
        interrupted = state;
        registry.stream().filter(feeder -> this != feeder).forEach(feeder -> {
            feeder.setInterrupted(state);
        });
    }

    public boolean getInterrupted() {
        return interrupted;
    }

    @Override
    public void close() throws IOException {
        setInterrupted(true);
        logger.info("closing ...");
    }

    @Override
    public boolean hasNext() {
        return !input.isEmpty();
    }

    @Override
    public URIPipelineElement next() {
        return new URIPipelineElement().set(input.poll());
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

    @Override
    public Thread shutdownHook() {
        return new Thread() {
            public void run() {
                for (Feeder feeder : registry) {
                    try {
                        if (feeder.scheduledThreadPoolExecutor != null) {
                            logger.info("shutting down scheduler");
                            feeder.scheduledThreadPoolExecutor.shutdownNow();
                            feeder.scheduledThreadPoolExecutor = null;
                        }
                        if (feeder.executor != null) {
                            logger.info("shutting down executor");
                            feeder.executor.shutdown();
                            feeder.executor = null;
                        }
                    } catch (InterruptedException e) {
                        logger.warn("interrupted", e);
                    } catch (IOException e) {
                        logger.error(e.getMessage(), e);
                    }
                }
                if (ingest != null) {
                    logger.info("shutting down ingester");
                    try {
                        ingest.stopBulk();
                        ingest.updateReplicaLevel(settings.getAsInt("replica", 0));
                        ingest.shutdown();
                    } catch (IOException e) {
                        logger.error(e.getMessage(), e);
                    }
                }
                logger.info("shutdown completed");
            }
        };
    }

    protected void active(URI uri) throws IOException {
        if (riverState != null && uri != null) {
            riverState.setName(uri.toString())
                    .setTimestamp(new Date())
                    .setActive(true)
                    .setCounter(counter.incrementAndGet())
                    .save(ingest.client());
        }
    }

    protected void inactive(URI uri) throws IOException {
        if (riverState != null && uri != null) {
            riverState.setName(uri.toString())
                    .setTimestamp(new Date())
                    .setActive(false)
                    .save(ingest.client());
        }
    }

    protected abstract PipelineProvider<P> pipelineProvider();

    protected abstract void process(URI uri) throws Exception;

}
