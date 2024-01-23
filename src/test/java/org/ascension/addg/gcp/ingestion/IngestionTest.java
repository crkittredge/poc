package org.ascension.addg.gcp.ingestion;

import ch.qos.logback.classic.Level;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PBegin;
import org.ascension.addg.gcp.BaseTest;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;
import org.ascension.addg.gcp.ingestion.core.Utils;
import org.ascension.addg.gcp.ingestion.read.Reader;
import org.ascension.addg.gcp.ingestion.transform.Transformer;
import org.ascension.addg.gcp.ingestion.write.Writer;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;

import java.util.Objects;

import static org.mockito.Mockito.*;

/**
 * Common functionality for tests
 */
@RunWith(Enclosed.class)
public abstract class IngestionTest {

    public abstract static class Setup extends BaseTest {

        /**
         * Sets up the test
         * @param pipelineArgs command line arguments
         * @param pipelineOptionsClazz pipeline options class
         * @param loggerClazz logger class
         */
        protected Setup(String[] pipelineArgs, Class<? extends IngestionOptions> pipelineOptionsClazz, Class<?> loggerClazz) {
            super(pipelineArgs, pipelineOptionsClazz, loggerClazz);
        }

        @Test public abstract void executeTest();

        protected Reader<PBegin> readerMock;
        protected Writer writerMock;
        protected MockedStatic<?> readerStaticMock;
        protected MockedStatic<?> writerStaticMock;

        /**
         * Tests the positive scenario of the main() and run() methods
         * @param configString contents of config
         */
        public void mainRun(ValueProvider<String> configString) {
            var pipelineOptionsFactoryBuilderMock = mock(PipelineOptionsFactory.Builder.class);

            var pipelineSpy = spy(this.pipeline);
            var pipelineOptionsSpy = spy(this.pipelineOptions);
            var inputPCTuple = spy(this.getTestPCollectionTuple());
            var readerMock = this.readerMock;
            var writerMock = this.writerMock;

            if (configString.get() != null && !configString.get().isEmpty() &&
                    !Objects.requireNonNull(configString.get()).contains("csv-exception-invalid-step") &&
                    !Objects.requireNonNull(configString.get()).contains("csv-exception-invalid-options-class")) {
                when(pipelineOptionsFactoryBuilderMock.withValidation()).thenReturn(pipelineOptionsFactoryBuilderMock);
            }

            when(pipelineOptionsFactoryBuilderMock.withoutStrictParsing()).thenReturn(pipelineOptionsFactoryBuilderMock);
            when(pipelineOptionsFactoryBuilderMock.as(any())).thenReturn(pipelineOptionsSpy);

            try (var pipelineMock = mockStatic(Pipeline.class, CALLS_REAL_METHODS);
                 var pipelineOptionsFactoryMock = mockStatic(PipelineOptionsFactory.class, CALLS_REAL_METHODS)) {

                // main() mocks
                pipelineMock.when(() -> Pipeline.create(any(IngestionOptions.class))).thenReturn(pipelineSpy);
                this.utilsMock.when(() -> Utils.readFromGCS(ValueProvider.StaticValueProvider.of(Setup.CONFIG_PATH))).thenReturn(configString);
                pipelineOptionsFactoryMock.when(() -> PipelineOptionsFactory.fromArgs(Setup.PIPELINE_ARGS)).thenReturn(pipelineOptionsFactoryBuilderMock);
                pipelineOptionsFactoryMock.clearInvocations();

                // run() mocks

                if (configString.get() != null && !configString.get().isEmpty() &&
                        !Objects.requireNonNull(configString.get()).contains("csv-exception-invalid-step") &&
                        !Objects.requireNonNull(configString.get()).contains("csv-exception-invalid-options-class") &&
                        !Objects.requireNonNull(configString.get()).contains("csv-exception-no-read") &&
                        !Objects.requireNonNull(configString.get()).contains("csv-exception-bad-reader-impl") &&
                        !Objects.requireNonNull(configString.get()).contains("csv-exception-bad-writer-impl") &&
                        !Objects.requireNonNull(configString.get()).contains("csv-exception-bad-transformer-impl")) {
                    doReturn(inputPCTuple).when(pipelineSpy).apply(any(String.class), eq(readerMock));
                }

                if (configString.get() != null &&
                        !Objects.requireNonNull(configString.get()).contains("csv-exception-invalid-step") &&
                        !Objects.requireNonNull(configString.get()).contains("csv-exception-invalid-options-class") &&
                        !Objects.requireNonNull(configString.get()).contains("csv-exception-no-read") &&
                        !Objects.requireNonNull(configString.get()).contains("csv-positive-invalid-step-class") &&
                        !Objects.requireNonNull(configString.get()).contains("csv-exception-bad-reader-impl") &&
                        !Objects.requireNonNull(configString.get()).contains("csv-exception-bad-writer-impl") &&
                        !Objects.requireNonNull(configString.get()).contains("csv-exception-bad-transformer-impl")) {
                    doAnswer(RETURNS_SELF).when(inputPCTuple).apply(any(String.class), any());
                }

                // execute the pipeline
                Ingestion.main(Setup.PIPELINE_ARGS);

                if (configString.get() != null &&
                        !Objects.requireNonNull(configString.get()).contains("csv-exception-invalid-step") &&
                        !Objects.requireNonNull(configString.get()).contains("csv-exception-invalid-options-class") &&
                        !Objects.requireNonNull(configString.get()).contains("csv-exception-bad-reader-impl")) {

                    // do validations
                    pipelineOptionsFactoryMock.verify(() -> PipelineOptionsFactory.fromArgs(Setup.PIPELINE_ARGS), times(2));
                    pipelineMock.verify(() -> Pipeline.create(any(IngestionOptions.class)), times(1));

                    assertLog("Reading config from " + Setup.CONFIG_PATH, Level.INFO);

                    // optional pipeline options
                    verify(pipelineOptionsSpy, times(2)).getJobNameOverride();
                    verify(pipelineOptionsSpy, times(2)).getWaitUntilFinish();

                    // ReadSource
                    verify(pipelineSpy, times(1)).apply(any(String.class), eq(readerMock));
                    //assertLog("ReadSource", Level.INFO);

                    // InjectMetadata
                    verify(inputPCTuple, times(1)).apply(any(String.class), any(Transformer.class));

                    verify(inputPCTuple, times(1)).apply(any(String.class), eq(writerMock));

                    verify(pipelineSpy, times(1)).run();
                }
            }
        }
    }
}
