package org.ascension.addg.gcp.ingestion.read.bigquery;

import com.google.api.services.bigquery.model.TableReference;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.Row;
import org.ascension.addg.gcp.BaseTest;
import org.ascension.addg.gcp.ingestion.IngestionTest;
import org.ascension.addg.gcp.ingestion.core.IngestionConfig;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;
import org.ascension.addg.gcp.ingestion.read.Reader;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.runners.Parameterized.Parameters;
import static org.mockito.Mockito.*;

/**
 * Tests row-level scenarios for reading data from MongoDb
 */
@RunWith(Parameterized.class)
public class ReadBigQueryDoFnTest extends IngestionTest.Setup {
    private final String confFile;
    private final List<Row> expectedRows;
    private final List<Row> expectedBadRows;

    /**
     * Test parameters
     * @return parameter collection
     */
    @Parameters
    public static Collection<Object[]> data() {
        return List.of(new Object[][]{
                { "bq-positive-read-table.conf" },
                { "bq-positive-read-query.conf" },
                { "bq-exception-read-bad-config.conf" }
        });
    }

    /**
     * Set up the test configuration
     * @param confFile configuration file name
     */
    public ReadBigQueryDoFnTest(String confFile) {
        super(BaseTest.PIPELINE_ARGS, IngestionOptions.class, ReadBigQueryDoFn.class);
        this.confFile = confFile;

        this.expectedRows = new ArrayList<>();
        var schema = this.getTestSchema();

        for (var i = 0; i < 10; i++) {
            var row = Row.withSchema(schema).addValues("1", 2, 3.0).build();
            this.expectedRows.add(row);
        }

        this.expectedBadRows = List.of();
    }

    @Override
    public void executeTest() {
        var config = ConfigBeanFactory.create(ConfigFactory.parseString(BaseTest.readConfAsString("ingestion/" + this.confFile)).resolve(), IngestionConfig.class);
        var stepConfig = (ReadBigQueryStep) config.getSteps().get(0);
        var pipelineSpy = spy(this.pipeline.begin());

        try (var bqioStaticMock = mockStatic(BigQueryIO.class, CALLS_REAL_METHODS)) {
            var bqioReadMock = spy(BigQueryIO.TypedRead.class);

            if (this.confFile.equals("bq-exception-read-bad-config.conf")) {
                this.pipeline.enableAbandonedNodeEnforcement(false);

                doReturn(this.getTestRowPCollection()).when(pipelineSpy).apply(eq("Read from BigQuery"), any());
                var exception = assertThrows(InvocationTargetException.class, () -> Reader.of(stepConfig, this.pipelineOptions));

                assertTrue(exception.getCause() instanceof BigQueryReader.ConfigurationException);
                assertEquals("Either query or projectId/datasetId/tableId must be provided", exception.getCause().getMessage());

            } else {
                doReturn(bqioReadMock).when(bqioReadMock).usingStandardSql();


                if (this.confFile.equals("bq-positive-read-table.conf")) {
                    doReturn(bqioReadMock).when(bqioReadMock).from(any(TableReference.class));
                } else if (this.confFile.equals("bq-positive-read-query.conf")) {
                    doReturn(bqioReadMock).when(bqioReadMock).fromQuery(any(String.class));
                }

                doReturn(this.getTestRowPCollection()).when(pipelineSpy).apply(eq("Read from BigQuery"), any());
                var reader = Reader.of(stepConfig, this.pipelineOptions);
                bqioStaticMock.when(() -> BigQueryIO.read(any())).thenReturn(bqioReadMock);
                var result = pipelineSpy.apply("ReadSource", reader);

                // check good records against expected
                PAssert.that(result.get(reader.getRecordsTag())).containsInAnyOrder(this.expectedRows);

                // for bigquery we don't have bad records
                PAssert.that(result.get(reader.getBadRecordsTag())).containsInAnyOrder(this.expectedBadRows);
                this.pipeline.run();
            }


        } catch (ReflectiveOperationException e) {
            fail(e.getMessage());
        }
    }
}
