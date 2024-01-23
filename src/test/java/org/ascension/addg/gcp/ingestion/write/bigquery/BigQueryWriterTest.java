package org.ascension.addg.gcp.ingestion.write.bigquery;

import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.ascension.addg.gcp.BaseTest;
import org.ascension.addg.gcp.ingestion.IngestionTest;
import org.ascension.addg.gcp.ingestion.core.IngestionConfig;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;
import org.ascension.addg.gcp.ingestion.read.ReadStep;
import org.ascension.addg.gcp.ingestion.transform.Transformer;
import org.ascension.addg.gcp.ingestion.transform.metadata.MetadataStep;
import org.ascension.addg.gcp.ingestion.write.Writer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentMatchers;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.Parameters;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class BigQueryWriterTest extends IngestionTest.Setup {
    private final String confFile;
    private final PCollectionTuple inputData;

    /**
     * Test parameters
     * @return parameter collection
     */
    @Parameters
    public static Collection<Object[]> data() {
        return List.of(new Object[][]{
                { "bq-positive-write-file-loads.conf" },
                { "bq-positive-write-storage-write-api.conf" },
                { "bq-positive-write-storage-api-alo.conf" },
                { "bq-positive-write-streaming-inserts.conf" }
        });
    }

    /**
     * Set up the test configuration
     * @param confFile configuration file name
     */
    public BigQueryWriterTest(String confFile) {
        super(BaseTest.PIPELINE_ARGS, IngestionOptions.class, BigQueryWriter.class);

        this.confFile = confFile;
        var inputRows = new ArrayList<Row>();

        if (this.confFile.equals("bq-positive-write-file-loads.conf")) {
            this.pipelineOptions.setBqLandProject(ValueProvider.StaticValueProvider.of("arg_project"));
            this.pipelineOptions.setBqLandDataset(ValueProvider.StaticValueProvider.of("arg_dataset"));
        } else {
            this.pipelineOptions.setBqLandProject(null);
            this.pipelineOptions.setBqLandDataset(null);
        }

        var inputSchema = Schema.builder()
                .addNullableInt32Field("id")
                .addNullableStringField("name")
                .addNullableBooleanField("boolean_value")
                .addNullableFloatField("float_value")
                .addNullableInt64Field("long_value")
                .addNullableDateTimeField("timestamp_value")
                .setOptions(Schema.Options.builder().setOption(ReadStep.OUTPUT_TABLE_FIELD, Schema.FieldType.STRING, "default").build())
                .build();

        for (int i = 0; i < 20; i++) {
            inputRows.add(Row.withSchema(inputSchema)
                    .addValues(i, "value_" + i, true, 5.5F, i * 800000L,
                            Instant.ofEpochMilli(LocalDateTime.of(2023, 1, 2, 12, 5, 0).toInstant(ZoneOffset.UTC).toEpochMilli()))
                    .build());
        }

        this.inputData = PCollectionTuple.of(this.recordsTag, this.pipeline.apply(Create.of(inputRows))).and(this.badRecordsTag, this.pipeline.apply(Create.empty(RowCoder.of(inputSchema))));
    }


    @Override
    public void executeTest() {

        var bqioMock = mock(BigQueryIO.Write.class);
        doReturn(bqioMock).when(bqioMock).withWriteDisposition(any());
        doReturn(bqioMock).when(bqioMock).withCreateDisposition(any());
        doReturn(bqioMock).when(bqioMock).withMethod(any());
        doReturn(bqioMock).when(bqioMock).withFormatFunction(any());
        doReturn(bqioMock).when(bqioMock).to(any(BigQueryDestination.class));

        try (var bqioStaticMock = mockStatic(BigQueryIO.class)) {
            bqioStaticMock.when(BigQueryIO::write).thenReturn(bqioMock);

            var config = ConfigBeanFactory.create(ConfigFactory.parseString(BaseTest.readConfAsString("ingestion/" + this.confFile)).resolve(), IngestionConfig.class);
            var metadataConfig = (MetadataStep) config.getSteps().get(1);
            var writeConfig = (WriteBigQueryStep) config.getSteps().get(2);

            var transformer = Transformer.of(metadataConfig, this.recordsTag, this.badRecordsTag, this.schemaTag, this.badSchemaTag);
            var writer = Writer.of(writeConfig, this.pipelineOptions, this.recordsTag, this.badRecordsTag, this.schemaTag, this.badSchemaTag);

            var result = this.inputData.apply("InjectMetadata", transformer);
            var tupleSpy = spy(result);
            var recordSpy = spy(tupleSpy.get(this.recordsTag));
            var badRecordSpy = spy(tupleSpy.get(this.badRecordsTag));
            var writeResultMock = mock(WriteResult.class);

            doReturn(recordSpy).when(tupleSpy).get(this.recordsTag);
            doReturn(badRecordSpy).when(tupleSpy).get(this.badRecordsTag);
            doReturn(writeResultMock).when(recordSpy).apply(any());
            doReturn(writeResultMock).when(badRecordSpy).apply(any());

            if (this.confFile.equals("bq-positive-write-file-loads.conf")) {
                doReturn(bqioMock).when(bqioMock).withSchemaUpdateOptions(ArgumentMatchers.<Set<BigQueryIO.Write.SchemaUpdateOption>>any());

            } else if (this.confFile.equals("bq-positive-write-storage-write-api.conf") || this.confFile.equals("bq-positive-write-storage-api-alo.conf")) {
                doAnswer(RETURNS_SELF).when(bqioMock).withAutoSchemaUpdate(anyBoolean());
                doReturn(bqioMock).when(bqioMock).ignoreUnknownValues();
                doReturn(bqioMock).when(bqioMock).withTriggeringFrequency(any(Duration.class));
                doReturn(bqioMock).when(bqioMock).withSchemaUpdateOptions(ArgumentMatchers.<Set<BigQueryIO.Write.SchemaUpdateOption>>any());

            } else if (this.confFile.equals("bq-positive-write-streaming-inserts.conf")) {
                doReturn(bqioMock).when(bqioMock).withExtendedErrorInfo();
                doReturn(bqioMock).when(bqioMock).withFailedInsertRetryPolicy(any(InsertRetryPolicy.class));
                doReturn(bqioMock).when(bqioMock).withSchemaUpdateOptions(ArgumentMatchers.<Set<BigQueryIO.Write.SchemaUpdateOption>>any());
            }

            tupleSpy.apply("WriteBigQueryTest", writer);

            verify(bqioMock, times(2)).withWriteDisposition(any());
            verify(bqioMock, times(2)).withCreateDisposition(any());
            verify(bqioMock, times(2)).withMethod(any());
            verify(bqioMock, times(2)).withFormatFunction(any());
            verify(bqioMock, times(2)).to(any(BigQueryDestination.class));

            if (this.confFile.equals("bq-positive-write-file-loads.conf")) {
                assertEquals("WriteBigQuery", writeConfig.getType());
                assertEquals("dstream_test", writeConfig.getBqLandTable());
                assertEquals(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED, writeConfig.getCreateDisposition());
                assertEquals(BigQueryIO.Write.WriteDisposition.WRITE_APPEND, writeConfig.getWriteDisposition());
                assertEquals(BigQueryIO.Write.Method.FILE_LOADS, writeConfig.getWriteMethod());
                assertEquals("meta_load_timestamp", writeConfig.getPartitionBy().getColumnName());
                assertEquals("DAY", writeConfig.getPartitionBy().getDataType());
            } else if (this.confFile.equals("bq-positive-write-storage-write-api.conf")) {
                assertEquals("WriteBigQuery", writeConfig.getType());
                assertEquals("some_project", writeConfig.getBqLandProject());
                assertEquals("some_dataset", writeConfig.getBqLandDataset());
                assertEquals("some_table", writeConfig.getBqLandTable());
                assertEquals(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED, writeConfig.getCreateDisposition());
                assertEquals(BigQueryIO.Write.WriteDisposition.WRITE_APPEND, writeConfig.getWriteDisposition());
                assertEquals(BigQueryIO.Write.Method.STORAGE_WRITE_API, writeConfig.getWriteMethod());
                assertEquals("meta_load_timestamp", writeConfig.getPartitionBy().getColumnName());
                assertEquals("DAY", writeConfig.getPartitionBy().getDataType());
            } else if (this.confFile.equals("bq-positive-write-storage-api-alo.conf")) {
                assertEquals("WriteBigQuery", writeConfig.getType());
                assertEquals("some_project", writeConfig.getBqLandProject());
                assertEquals("some_dataset", writeConfig.getBqLandDataset());
                assertEquals("some_table", writeConfig.getBqLandTable());
                assertEquals(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED, writeConfig.getCreateDisposition());
                assertEquals(BigQueryIO.Write.WriteDisposition.WRITE_APPEND, writeConfig.getWriteDisposition());
                assertEquals(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE, writeConfig.getWriteMethod());
                assertEquals("meta_load_timestamp", writeConfig.getPartitionBy().getColumnName());
                assertEquals("DAY", writeConfig.getPartitionBy().getDataType());
            } else {
                assertEquals("WriteBigQuery", writeConfig.getType());
                assertEquals("some_project", writeConfig.getBqLandProject());
                assertEquals("some_dataset", writeConfig.getBqLandDataset());
                assertEquals("some_table", writeConfig.getBqLandTable());
                assertEquals(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED, writeConfig.getCreateDisposition());
                assertEquals(BigQueryIO.Write.WriteDisposition.WRITE_APPEND, writeConfig.getWriteDisposition());
                assertEquals(BigQueryIO.Write.Method.STREAMING_INSERTS, writeConfig.getWriteMethod());
                assertEquals("meta_load_timestamp", writeConfig.getPartitionBy().getColumnName());
                assertEquals("DAY", writeConfig.getPartitionBy().getDataType());
            }

            this.pipeline.run();
        } catch (ReflectiveOperationException e) {
            fail(e.getMessage());
        }
    }
}
