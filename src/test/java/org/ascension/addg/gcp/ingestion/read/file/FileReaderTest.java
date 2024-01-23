package org.ascension.addg.gcp.ingestion.read.file;

import com.typesafe.config.*;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.*;
import org.ascension.addg.gcp.BaseTest;
import org.ascension.addg.gcp.ingestion.core.IngestionConfig;
import org.ascension.addg.gcp.ingestion.IngestionTest;
import org.ascension.addg.gcp.ingestion.core.Utils;
import org.ascension.addg.gcp.ingestion.read.Reader;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentMatchers;

import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.junit.runners.Parameterized.Parameters;

/**
 * Tests transform-level File reader flow
 */
@RunWith(Parameterized.class)
public class FileReaderTest extends IngestionTest.Setup {

    private final String compressionFormat;
    private final String dataFormat;

    @Parameters
    public static Collection<Object[]> data() {
        return List.of(new Object[][]{
                { "NONE", "CSV", "some_pattern", null },
                { "NONE", "CSV", "some_pattern", "some/path/pattern_file.txt" },
                { "ZIP", "CSV", "some_pattern", null },
                { "TAR", "CSV", "some_pattern", null },
                { "NONE", "JSON", "some_pattern", null },
                { "ZIP", "JSON", "some_pattern", null },
                { "TAR", "JSON", "some_pattern", null },
                { "NONE", "CSV", "some_pattern", null }
        });
    }

    public FileReaderTest(String compressionFormat, String dataFormat, String filePattern, String filePatternFile) {
        super(BaseTest.PIPELINE_ARGS, FileIngestionOptions.class, FileReader.class);
        this.compressionFormat = compressionFormat;
        this.dataFormat = dataFormat;

        var options = (FileIngestionOptions) this.pipelineOptions;
        options.setInputFilePattern(ValueProvider.StaticValueProvider.of(filePattern));
        options.setPatternsFromFile(ValueProvider.StaticValueProvider.of(filePatternFile));

        if (filePatternFile != null) {
            this.utilsMock.when(() -> Utils.readFromGCS(ValueProvider.StaticValueProvider.of(filePatternFile))).thenReturn(ValueProvider.StaticValueProvider.of("pattern_from_file"));
        }
    }

    @Override
    public void executeTest() {

        Config c;

        if (this.dataFormat.equals("JSON")) {
            c = ConfigFactory.parseString(BaseTest.readConfAsString("ingestion/json-positive.conf")).resolve();
        } else {
            c = ConfigFactory.parseString(BaseTest.readConfAsString("ingestion/csv-positive.conf")).resolve();
        }

        var config = ConfigBeanFactory.create(c, IngestionConfig.class);
        var stepConfig = (ReadFileStep) config.getSteps().get(0);
        stepConfig.setCompressionFormat(ReadFileStep.CompressionFormat.valueOf(this.compressionFormat));

        var configSpy = spy(stepConfig);
        var fileioMatcherMock = mock(FileIO.MatchAll.class);
        var filioReadMatchesMock = mock(FileIO.ReadMatches.class, RETURNS_SELF);
        var extractEntriesMock = mock(ReadArchiveDoFn.class);

        doReturn(filioReadMatchesMock).when(filioReadMatchesMock).withCompression(any());

        try (var fileIOStaticMock = mockStatic(FileIO.class);
             var extractEntriesStaticMock = mockStatic(ReadArchiveDoFn.class);) {
            fileIOStaticMock.when(FileIO::readMatches).thenReturn(filioReadMatchesMock);
            fileIOStaticMock.when(FileIO::matchAll).thenReturn(fileioMatcherMock);
            extractEntriesStaticMock.when(() -> ReadArchiveDoFn.getInstance(any())).thenReturn(extractEntriesMock);

            var pcMock = mock(PCollection.class);
            var pipelineSpy = spy(this.pipeline.begin());
            var readerSpy = spy(Reader.of(configSpy, this.pipelineOptions));
            var pcTupleSpy = spy(PCollectionTuple.of(readerSpy.getRecordsTag(), this.getTestRowPCollection())
                    .and(readerSpy.getBadRecordsTag(), this.getTestBadRowPCollection())
                    .and(readerSpy.getSchemaTag(), this.getTestSchemaPCollection()))
                    .and(readerSpy.getBadSchemaTag(), this.getTestSchemaPCollection());


            doReturn(pcMock).when(pipelineSpy).apply(eq("File List"), ArgumentMatchers.<Create.Values<String>>any());
            doReturn(pcMock).when(pcMock).apply(eq("Match Input Files"), any());
            doReturn(pcMock).when(pcMock).apply("Read Matched Files", filioReadMatchesMock);

            if (!this.compressionFormat.equals("NONE")) {
                doReturn(pcMock).when(pcMock).apply(eq("Read Archive Entries"), any());
                doReturn(pcMock).when(pcMock).apply(eq("Reshuffle Archive Entries"), any());
            }

            doReturn(pcTupleSpy).when(pcMock).apply(eq("Read Files"), any());

            var readResult = pipelineSpy.apply("ReadSource", readerSpy);
            this.pipeline.run().waitUntilFinish();

            verify(pipelineSpy, times(1)).apply(eq("File List"), ArgumentMatchers.<Create.Values<String>>any());
            verify(pcMock, times(1)).apply(eq("Match Input Files"), any());
            verify(pcMock, times(1)).apply("Read Matched Files", filioReadMatchesMock);

            if (!this.compressionFormat.equals("NONE")) {
                verify(pcMock, times(1)).apply(eq("Read Archive Entries"), any());
                verify(pcMock, times(1)).apply(eq("Reshuffle Archive Entries"), any());
            }

            verify(pcMock, times(1)).apply(eq("Read Files"), any());

            // make sure our mocking was good
            assertEquals(pcTupleSpy.get(readerSpy.getRecordsTag()), readResult.get(readerSpy.getRecordsTag()));
            assertEquals(pcTupleSpy.get(readerSpy.getBadRecordsTag()), readResult.get(readerSpy.getBadRecordsTag()));
        } catch (ReflectiveOperationException e) {
            fail(e.getMessage());
        }
    }
}