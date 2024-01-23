package org.ascension.addg.gcp.ingestion.read.file;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.*;
import org.ascension.addg.gcp.BaseTest;
import org.ascension.addg.gcp.ingestion.Ingestion;
import org.ascension.addg.gcp.ingestion.core.BaseStep;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;
import org.ascension.addg.gcp.ingestion.IngestionTest;
import org.ascension.addg.gcp.ingestion.core.StepMap;
import org.ascension.addg.gcp.ingestion.read.Reader;
import org.ascension.addg.gcp.ingestion.read.ReadStep;
import org.ascension.addg.gcp.ingestion.transform.TransformerStep;
import org.ascension.addg.gcp.ingestion.transform.Transformer;
import org.ascension.addg.gcp.ingestion.write.WriteStep;
import org.ascension.addg.gcp.ingestion.write.Writer;
import org.ascension.addg.gcp.ingestion.write.bigquery.BigQueryWriter;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

import static org.junit.Assert.*;
import static org.junit.runners.Parameterized.Parameters;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Tests transform-level File reader flow
 */
@RunWith(Parameterized.class)
public class FileIngestionTest extends IngestionTest.Setup {

    private final ValueProvider<String> config;
    private final String configFile;

    @Override public void tearDown() {
        super.tearDown();
        this.readerStaticMock.close();
        this.writerStaticMock.close();
    }

    @Parameters
    public static Collection<Object[]> data() {
        return List.of(new Object[][]{
                { true, true, "csv-positive.conf" },                        // positive scenario with wait
                { false, true, "csv-positive.conf" },                       // positive scenario without wait
                { true, false, "csv-positive.conf" },                       // positive scenario having bq configs from options
                { true, true, "csv-exception-no-read.conf" },               // applying metadata without first reading source data
                { true, true, "csv-exception-invalid-step.conf" },          // invalid step encountered
                { true, true, "csv-positive-invalid-step-class.conf" },     // invalid step class encountered
                { true, true, "csv-positive-no-bq-land.conf" },             // bqLandTable not defined in conf
                { true, true, "csv-exception-invalid-options-class.conf" }, // invalid options class implementation
                { true, true, "csv-exception-bad-reader-impl.conf" },        // invalid reader class implementation
                { true, true, "csv-exception-bad-writer-impl.conf" },        // invalid writer class implementation
                { true, true, "csv-exception-bad-transformer-impl.conf" }    // invalid transformer class implementation
        });
    }

    /**
     * Tests file-based ingestion without testing the readers
     * @param waitUntilFinish block until pipeline is complete
     * @param bqConfigFromConf read from .conf vs pipeline options
     * @param configFile config file name
     */
    public FileIngestionTest(boolean waitUntilFinish, boolean bqConfigFromConf, String configFile) {
        super(BaseTest.PIPELINE_ARGS, FileIngestionOptions.class, Ingestion.class);
        this.readerMock = mock(FileReader.class);
        this.writerMock = mock(BigQueryWriter.class);
        this.readerStaticMock = mockStatic(Reader.class);
        this.writerStaticMock = mockStatic(Writer.class);
        this.configFile = configFile;
        this.config = this.configFile == null || this.configFile.isEmpty() ? ValueProvider.StaticValueProvider.of(null) : BaseTest.readConf("ingestion/" + this.configFile);

        var options = (FileIngestionOptions) this.pipelineOptions;

        options.setWaitUntilFinish(ValueProvider.StaticValueProvider.of(Boolean.toString(waitUntilFinish)));

        if (bqConfigFromConf) {
            options.setBqLandProject(null);
            options.setBqLandDataset(null);
        } else {
            options.setBqLandProject(ValueProvider.StaticValueProvider.of("arg_project"));
            options.setBqLandDataset(ValueProvider.StaticValueProvider.of("arg_dataset"));
        }
    }

    @StepMap(name = "SomeInvalidStepClass")
    public static class InvalidStep extends BaseStep {
        public InvalidStep() {
            super("");
        }
    }

    @StepMap(name = "SomeInvalidReaderClassStep")
    public static class InvalidReaderClassStep extends ReadStep {

        private static class BadReader extends Reader<PBegin> {
            protected BadReader(ReadStep config, IngestionOptions options) {
                super(config, options);
            }
            @Override
            public @NonNull PCollectionTuple expand(@NonNull PBegin input) {
                return mock(PCollectionTuple.class);
            }
        }

        public InvalidReaderClassStep() {
            super("some.invalid.class", BadReader.class);
        }
    }

    @StepMap(name = "SomeInvalidTransformerClassStep")
    public static class InvalidTransformerClassStep extends TransformerStep {

        private static class BadTransformer extends Transformer {
            protected BadTransformer(TransformerStep config) {
                super(config, null, null, null, null);
            }
            @Override
            public @NonNull PCollectionTuple expand(@NonNull PCollectionTuple input) {
                return mock(PCollectionTuple.class);
            }
        }

        public InvalidTransformerClassStep() {
            super("some.invalid.class");
        }
    }

    @StepMap(name = "SomeInvalidWriterClassStep")
    public static class InvalidWriterClassStep extends WriteStep {

        private static class BadWriter extends Writer {
            protected BadWriter(WriteStep config) {
                super(config, null, null, null, null);
            }
            @Override
            public @NonNull PDone expand(@NonNull PCollectionTuple input) {
                return mock(PDone.class);
            }

            @Override
            protected void writeRows(PCollection<Row> input, PCollectionView<Map<String, Schema>> schemaSideInput, boolean isErrorDestination) {
                //do nothing
            }
        }

        public InvalidWriterClassStep() {
            super("some.invalid.class", BadWriter.class);
        }
    }

    @Override
    public void executeTest() {
        try {

            if (this.configFile.equals("csv-exception-bad-reader-impl.conf")) {
                this.pipeline.enableAbandonedNodeEnforcement(false);
                this.readerStaticMock.when(() -> Reader.of(any(), any())).thenAnswer(CALLS_REAL_METHODS);

                var options = (FileIngestionOptions) this.pipelineOptions;
                options.setInputFilePattern(ValueProvider.StaticValueProvider.of("some_pattern"));
                options.setPatternsFromFile(ValueProvider.StaticValueProvider.of(null));

                var exception = assertThrows(Ingestion.InvalidStepException.class, () -> super.mainRun(this.config));
                assertEquals("org.ascension.addg.gcp.ingestion.read.file.FileIngestionTest$InvalidReaderClassStep implementation is not valid: " +
                        "org.ascension.addg.gcp.ingestion.read.file.FileIngestionTest$InvalidReaderClassStep$BadReader.<init>(org.ascension.addg.gcp.ingestion.read.ReadStep,org.ascension.addg.gcp.ingestion.core.IngestionOptions)", exception.getMessage());

                this.readerStaticMock.verify(() -> Reader.of(any(), any()), times(1));
                this.writerStaticMock.verify(() -> Writer.of(any(), any(), any(), any(), any(), any()), never());

            } else if (this.configFile.equals("csv-exception-bad-writer-impl.conf")) {
                this.pipeline.enableAbandonedNodeEnforcement(false);
                this.readerStaticMock.when(() -> Reader.of(any(), any())).thenAnswer(CALLS_REAL_METHODS);
                this.writerStaticMock.when(() -> Writer.of(any(), any(), any(), any(), any(), any())).thenAnswer(CALLS_REAL_METHODS);

                var options = (FileIngestionOptions) this.pipelineOptions;
                options.setInputFilePattern(ValueProvider.StaticValueProvider.of("some_pattern"));
                options.setPatternsFromFile(ValueProvider.StaticValueProvider.of(null));

                var exception = assertThrows(Ingestion.InvalidStepException.class, () -> super.mainRun(this.config));
                assertEquals("org.ascension.addg.gcp.ingestion.read.file.FileIngestionTest$InvalidWriterClassStep implementation is not valid: " +
                        "org.ascension.addg.gcp.ingestion.read.file.FileIngestionTest$InvalidWriterClassStep$BadWriter.<init>(org.ascension.addg.gcp.ingestion.write.WriteStep," +
                        "org.ascension.addg.gcp.ingestion.core.IngestionOptions,org.apache.beam.sdk.values.TupleTag,org.apache.beam.sdk.values.TupleTag,org.apache.beam.sdk.values.TupleTag,org.apache.beam.sdk.values.TupleTag)", exception.getMessage());

                this.readerStaticMock.verify(() -> Reader.of(any(), any()), times(1));
                this.writerStaticMock.verify(() -> Writer.of(any(), any(), any(), any(), any(), any()), times(1));

            } else if (this.configFile.equals("csv-exception-bad-transformer-impl.conf")) {
                this.pipeline.enableAbandonedNodeEnforcement(false);
                this.readerStaticMock.when(() -> Reader.of(any(), any())).thenAnswer(CALLS_REAL_METHODS);

                var options = (FileIngestionOptions) this.pipelineOptions;
                options.setInputFilePattern(ValueProvider.StaticValueProvider.of("some_pattern"));
                options.setPatternsFromFile(ValueProvider.StaticValueProvider.of(null));

                var exception = assertThrows(BaseStep.InvalidImplementationException.class, () -> super.mainRun(this.config));
                assertEquals("java.lang.ClassNotFoundException: some.invalid.class", exception.getMessage());

                this.readerStaticMock.verify(() -> Reader.of(any(), any()), times(1));
                this.writerStaticMock.verify(() -> Writer.of(any(), any(), any(), any(), any(), any()), never());

            } else {

                this.readerStaticMock.when(() -> Reader.of(any(), any())).thenReturn(this.readerMock);
                this.writerStaticMock.when(() -> Writer.of(any(), any(), any(), any(), any(), any())).thenReturn(this.writerMock);

                if (this.configFile.equals("csv-exception-invalid-step.conf")) {
                    this.pipeline.enableAbandonedNodeEnforcement(false);
                    var exception = assertThrows(com.typesafe.config.ConfigException.BadBean.class, () -> super.mainRun(this.config));
                    assertEquals("Step type SOMEINVALIDSTEP is not valid", exception.getCause().getCause().getMessage());

                } else if (this.configFile.equals("csv-exception-no-read.conf")) {
                    this.pipeline.enableAbandonedNodeEnforcement(false);
                    var exception = assertThrows(Ingestion.InsufficientDataException.class, () -> super.mainRun(this.config));
                    assertEquals("Call to step InjectMetadata, but no source data has yet been read", exception.getMessage());

                } else if (this.configFile.equals("csv-positive-invalid-step-class.conf")) {
                    this.pipeline.enableAbandonedNodeEnforcement(false);
                    var exception = assertThrows(Ingestion.InvalidStepException.class, () -> super.mainRun(this.config));
                    assertEquals("org.ascension.addg.gcp.ingestion.read.file.FileIngestionTest$InvalidStep is not a valid step class. Step class must inherit from ReadStep, TransformStep, or WriteStep", exception.getMessage());

                } else if (this.configFile.equals("csv-exception-invalid-options-class.conf")) {
                    this.pipeline.enableAbandonedNodeEnforcement(false);
                    var exception = assertThrows(Ingestion.InvalidOptionsClassException.class, () -> super.mainRun(this.config));
                    assertEquals("The specified pipeline options class is not valid: org.ascension.addg.gcp.ingestion.read.file.InvalidOptionsClass", exception.getMessage());

                } else {
                    super.mainRun(this.config);

                    this.readerStaticMock.verify(() -> Reader.of(any(), any()), times(1));
                    this.writerStaticMock.verify(() -> Writer.of(any(), any(), any(), any(), any(), any()), times(1));
                }
            }
        } catch (Exception e) {
            fail(e.toString());
        }
    }
}