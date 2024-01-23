package org.ascension.addg.gcp.ingestion.read.snowflake;

import org.ascension.addg.gcp.BaseTest;
import org.ascension.addg.gcp.ingestion.Ingestion;
import org.ascension.addg.gcp.ingestion.IngestionTest;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;
import org.ascension.addg.gcp.ingestion.read.ReadStep;
import org.ascension.addg.gcp.ingestion.read.Reader;
import org.ascension.addg.gcp.ingestion.write.Writer;
import org.ascension.addg.gcp.ingestion.write.bigquery.BigQueryWriter;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Tests snowflake ingestion
 */
public class SnowflakeIngestionTest extends IngestionTest.Setup {

    /**
     * Set up the test
     */
    public SnowflakeIngestionTest() {
        super(BaseTest.PIPELINE_ARGS, IngestionOptions.class, Ingestion.class);
        this.readerMock = mock(SnowflakeReader.class);
        this.writerMock = mock(BigQueryWriter.class);
        this.readerStaticMock = mockStatic(Reader.class);
        this.writerStaticMock = mockStatic(Writer.class);
        this.pipelineOptions.setBqLandProject(null);
        this.pipelineOptions.setBqLandDataset(null);
    }

    @Override public void tearDown() {
        super.tearDown();
        this.readerStaticMock.close();
        this.writerStaticMock.close();
    }

    /**
     * Tests the positive scenario of the main() and run() methods
     */
    @Override
    public void executeTest() {
        try {
            var configString = BaseTest.readConf("ingestion/snowflake-positive.conf");
            this.readerStaticMock.when(() -> Reader.of(any(ReadStep.class), any(IngestionOptions.class))).thenReturn(this.readerMock);
            this.writerStaticMock.when(() -> Writer.of(any(), any(), any(), any(), any(), any())).thenReturn(this.writerMock);
            super.mainRun(configString);
            this.readerStaticMock.verify(() -> Reader.of(any(ReadStep.class), any(IngestionOptions.class)), times(1));
            this.writerStaticMock.verify(() -> Writer.of(any(), any(), any(), any(), any(), any()), times(1));
        } catch (Exception e) {
            fail(e.toString());
        }
    }
}
