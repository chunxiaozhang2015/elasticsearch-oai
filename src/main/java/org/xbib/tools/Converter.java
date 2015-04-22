package org.xbib.tools;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xbib.common.settings.Settings;
import org.xbib.io.archive.file.Finder;
import org.xbib.metrics.MeterMetric;
import org.xbib.pipeline.AbstractPipeline;
import org.xbib.pipeline.Pipeline;
import org.xbib.pipeline.PipelineException;
import org.xbib.pipeline.PipelineProvider;
import org.xbib.pipeline.PipelineRequest;
import org.xbib.pipeline.element.URIPipelineElement;
import org.xbib.pipeline.simple.MetricSimplePipelineExecutor;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.xbib.common.settings.ImmutableSettings.settingsBuilder;

public abstract class Converter<T, R extends PipelineRequest, P extends Pipeline<T, R>>
        extends AbstractPipeline<URIPipelineElement, PipelineException> implements CommandLineInterpreter {

    private final static Logger logger = LogManager.getLogger(Converter.class.getSimpleName());

    private final static URIPipelineElement uriPipelineElement = new URIPipelineElement();

    protected Reader reader;

    protected Writer writer;

    protected static Settings settings;

    protected static Queue<URI> input;

    protected MetricSimplePipelineExecutor<T, R, P> executor;

    private boolean done = false;

    public Converter<T, R, P> reader(Reader reader) {
        this.reader = reader;
        settings = settingsBuilder().loadFromReader(reader).build();
        return this;
    }

    public Converter<T, R, P> writer(Writer writer) {
        this.writer = writer;
        return this;
    }

    protected Converter<T, R, P> prepare() throws IOException {
        if (settings.get("uri") != null) {
            input = new ConcurrentLinkedQueue<URI>();
            input.add(URI.create(settings.get("uri")));
            // parallel URI connection possible?
            if (settings.getAsBoolean("parallel", false)) {
                for (int i = 1; i < settings.getAsInt("concurrency", 1); i++) {
                    input.add(URI.create(settings.get("uri")));
                }
            }
        } else {
            input = new Finder(settings.get("pattern"))
                    .find(settings.get("path"))
                    .pathSorted(settings.getAsBoolean("isPathSorted", false))
                    .chronologicallySorted(settings.getAsBoolean("isChronologicallySorted", false))
                    .getURIs();
        }
        logger.info("input = {}", input);
        return this;
    }

    public void run() throws Exception {
        try {
            logger.info("preparing with settings {}", settings.getAsMap());
            prepare();
            logger.info("executing");
            //metric pipeline setExecutor only uses concurrency over different URIs
            // in the input queue, not with a single URI input
            executor = new MetricSimplePipelineExecutor<T, R, P>()
                    .setConcurrency(settings.getAsInt("concurrency", 1))
                    .setPipelineProvider(pipelineProvider())
                    .prepare()
                    .execute()
                    .waitFor();
            logger.info("execution completed");
        } finally {
            cleanup();
            if (executor != null) {
                executor.shutdown();
            }
        }
    }

    public Converter<T, R, P> run(Settings newSettings, Queue<URI> newInput) throws Exception {
        try {
            settings = newSettings;
            input = newInput;
            logger.info("executing with settings {} and input {}", settings, input);
            executor = new MetricSimplePipelineExecutor<T, R, P>()
                    .setConcurrency(settings.getAsInt("concurrency", 1))
                    .setPipelineProvider(pipelineProvider())
                    .prepare();
            executor.execute()
                    .waitFor();
            logger.info("execution completed");
        } finally {
            cleanup();
            if (executor != null) {
                executor.shutdown();
            }
        }
        return this;
    }

    protected Converter<T, R, P> cleanup() throws IOException {
        return this;
    }

    @Override
    public void close() throws IOException {
        logger.info("pipeline close (no op)");
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
            logger.info("done is true");
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
