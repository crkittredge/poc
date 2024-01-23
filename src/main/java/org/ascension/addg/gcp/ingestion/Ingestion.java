package org.ascension.addg.gcp.ingestion;

import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.ascension.addg.gcp.ingestion.core.IngestionConfig;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;
import org.ascension.addg.gcp.ingestion.core.Utils;
import org.ascension.addg.gcp.ingestion.read.ReadStep;
import org.ascension.addg.gcp.ingestion.transform.TransformerStep;
import org.ascension.addg.gcp.ingestion.write.WriteStep;
import org.ascension.addg.gcp.ingestion.read.Reader;
import org.ascension.addg.gcp.ingestion.transform.Transformer;
import org.ascension.addg.gcp.ingestion.write.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Objects;

/**
 * Abstraction for an Ingestion pipeline
 */
public class Ingestion implements Serializable {

    // logger definition
    protected static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // parsed configuration object
    protected IngestionConfig config;

    /**
     * Raised for configuration errors
     */
    public static class InvalidStepException extends RuntimeException {

        /**
         * Instantiates a new exception object
         * @param m String error message
         */
        public InvalidStepException(String m) {
            super(m);
        }
    }

    /**
     * Raised when a transformation is attempted without first reading some source data
     */
    public static class InsufficientDataException extends RuntimeException {

        /**
         * Instantiates a new exception object
         * @param m String error message
         */
        public InsufficientDataException(String m) {
            super(m);
        }
    }

    /**
     * Raised for invalid pipeline options class
     */
    public static class InvalidOptionsClassException extends RuntimeException {

        /**
         * Instantiates a new exception object
         * @param m String error message
         */
        public InvalidOptionsClassException(String m) {
            super(m);
        }
    }

    /**
     * Executes the ingestion flow
     * @param <I> child class of IngestionOptions
     * @param options pipeline options
     * @return {@link PipelineResult}
     */
    public <I extends IngestionOptions> PipelineResult run(I options) {
        var p = Pipeline.create(options);

        PCollectionTuple pc = null;
        Reader<PBegin> reader = null;

        LOG.info("Running pipeline configuration: {}", this.config.getName());

        // iterate over the steps in the config
        for (var step : this.config.getSteps()) {
            try {

                // we need to read from the source before any other steps
                if (step instanceof ReadStep cfg) {
                    reader = Reader.of(cfg, options);
                    pc = p.apply(step.getType(), Objects.requireNonNull(reader));

                // if we have already read from the source
                } else if (reader != null) {
                    if (step instanceof TransformerStep cfg) {
                        pc = pc.apply(step.getType(), Transformer.of(cfg, reader.getRecordsTag(), reader.getBadRecordsTag(), reader.getSchemaTag(), reader.getBadSchemaTag()));

                    } else if (step instanceof WriteStep cfg) {
                        pc.apply(step.getType(), Writer.of(cfg, options, reader.getRecordsTag(), reader.getBadRecordsTag(), reader.getSchemaTag(), reader.getBadSchemaTag()));

                    } else {
                        throw new InvalidStepException(step.getClass().getName() + " is not a valid step class. Step class must inherit from ReadStep, TransformStep, or WriteStep");
                    }
                } else {
                    throw new InsufficientDataException("Call to step " + step.getType() + ", but no source data has yet been read");
                }
            } catch (ReflectiveOperationException e) {
                throw new InvalidStepException(step.getClass().getName() + " implementation is not valid: " + e.getMessage());
            }
        }

        return p.run();
    }

    /**
     * Instantiates an Ingestion object with a provided configuration and custom IngestionOptions implementation
     * @param config typesafe config
     */
    protected Ingestion(IngestionConfig config) {
        this.config = config;
    }

    /**
     * Entrypoint for common ingestion flows
     * @param args command-line arguments
     */
    public static void main(String[] args) {
        LOG.trace("parsing arguments");
        var waitUntilFinish = true;

        FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create());

        // read arguments to parse the config path, but defer validation for now
        var options = PipelineOptionsFactory.fromArgs(args).withoutStrictParsing().as(IngestionOptions.class);

        // read configuration file
        LOG.info("Reading config from {}", options.getPipelineConfig().get());
        var configStr = Utils.readFromGCS(options.getPipelineConfig()).get();

        var config = ConfigFactory.parseString(configStr).resolve();
        var configObj = ConfigBeanFactory.create(config, IngestionConfig.class);
        var obj = new Ingestion(configObj);

        try {
            @SuppressWarnings("unchecked")
            var optCls = (Class<? extends IngestionOptions>) Class.forName(configObj.getPipelineOptionsClass());
            options = PipelineOptionsFactory.fromArgs(args).withValidation().as(optCls);

            // Use override job name if provided.  This is for overriding the automatically unique jobName
            // that airflow operator generates.
            if (options.getJobNameOverride().isAccessible()) {
                options.setJobName(Objects.requireNonNull(options.getJobNameOverride().get()));
            }

            // default to blocking exec if option not specified for compatibility with existing jobs
            if (options.getWaitUntilFinish().isAccessible()) {
                waitUntilFinish = Boolean.parseBoolean(options.getWaitUntilFinish().get());
            }

            // execute the job
            var result = obj.run(options);

            if (waitUntilFinish) {
                result.waitUntilFinish();
            }
        } catch (ClassNotFoundException e) {
            throw new InvalidOptionsClassException("The specified pipeline options class is not valid: " + configObj.getPipelineOptionsClass());
        }
    }
}
