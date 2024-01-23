package org.ascension.addg.gcp.ingestion.transform;

import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.ascension.addg.gcp.BaseTest;
import org.ascension.addg.gcp.ingestion.core.BaseStep;
import org.ascension.addg.gcp.ingestion.core.IngestionConfig;
import org.ascension.addg.gcp.ingestion.IngestionTest;
import org.ascension.addg.gcp.ingestion.core.Utils;
import org.ascension.addg.gcp.ingestion.read.file.FileIngestionOptions;
import org.ascension.addg.gcp.ingestion.read.file.ReadFileDoFn;
import org.ascension.addg.gcp.ingestion.read.file.ReadFileStep;
import org.ascension.addg.gcp.ingestion.transform.athena_metadata.AthenaMetadataDoFn;
import org.ascension.addg.gcp.ingestion.transform.metadata.MetadataDoFn;
import org.joda.time.Instant;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Tests Metadata transformations
 */
@RunWith(Parameterized.class)
public class MetadataTest extends IngestionTest.Setup {

    private final String confFile;
    private final String sourceFile;

    /**
     * Test parameters
     * @return parameter collection
     */
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return List.of(new Object[][]{
                { "csv-positive.conf", "csv-with-header.csv" },
                { "csv-positive-athena.conf", "csv-with-header-athena_20230101_12345.csv" },
                { "csv-exception-athena-parse-error.conf", "csv-with-header-athena_20230101_12345.csv" },
                { "csv-exception-athena-bad-filename.conf", "csv-with-header-athena.csv" }
        });
    }

    /**
     * Set up the test configuration
     * @param confFile configuration file name
     * @param sourceFile source file name
     */
    public MetadataTest(String confFile, String sourceFile) {
        super(BaseTest.PIPELINE_ARGS, FileIngestionOptions.class, ReadFileDoFn.class);
        this.confFile = confFile;
        this.sourceFile = sourceFile;
    }

    @Override
    public void executeTest() {

        try (var athenaDnFnMockedStatic = mockStatic(AthenaMetadataDoFn.class, CALLS_REAL_METHODS)) {
            final Instant TESTING_DT = Instant.parse("2023-11-24T00:00:00.000Z");
            Schema inputSchema;
            List<Row> inputRows;
            Schema expectedSchema;
            List<Row> expectedRows = null;

            inputSchema = Schema.builder()
                    .addNullableStringField("field1")
                    .addNullableStringField("field2")
                    .addNullableStringField("field3")
                    .setOptions(Schema.Options.builder()
                            .setOption(ReadFileStep.OUTPUT_TABLE_FIELD, Schema.FieldType.STRING, "default")
                            .setOption(ReadFileStep.FILE_NAME_FIELD, Schema.FieldType.STRING, this.sourceFile).build())
                    .build();

            inputRows = List.of(
                    Row.withSchema(inputSchema).addValues("A", "B", "1").build(),
                    Row.withSchema(inputSchema).addValues("C", "D", "2").build(),
                    Row.withSchema(inputSchema).addValues("D", "E", "3").build());

            if (this.confFile.equals("csv-positive.conf")) {
                expectedSchema = Schema.builder()
                        .addStringField(ReadFileStep.FILE_NAME_FIELD)
                        .addDateTimeField("meta_load_timestamp")
                        .addNullableStringField("RENAMED_FIELD_1")
                        .addNullableStringField("field2")
                        .addNullableStringField("field3")
                        .setOptions(Schema.Options.builder()
                                .setOption(ReadFileStep.OUTPUT_TABLE_FIELD, Schema.FieldType.STRING, "default")
                                .setOption(ReadFileStep.FILE_NAME_FIELD, Schema.FieldType.STRING, this.sourceFile)
                                .setOption("meta_load_timestamp", Schema.FieldType.DATETIME, TESTING_DT).build())
                        .build();

                expectedRows = List.of(
                        Row.withSchema(expectedSchema).addValues(this.sourceFile, TESTING_DT, "A", "B", "1").build(),
                        Row.withSchema(expectedSchema).addValues(this.sourceFile, TESTING_DT, "C", "D", "2").build(),
                        Row.withSchema(expectedSchema).addValues(this.sourceFile, TESTING_DT, "D", "E", "3").build());

            } else if (this.confFile.equals("csv-positive-athena.conf")) {

                athenaDnFnMockedStatic.when(() -> AthenaMetadataDoFn.getMinistryID(this.sourceFile)).thenAnswer(CALLS_REAL_METHODS);
                this.utilsMock.when(() -> Utils.readFromGCS(any())).thenReturn(ValueProvider.StaticValueProvider.of("{\"12345\": \"MIN_VAL\"}"));
                athenaDnFnMockedStatic.when(() -> AthenaMetadataDoFn.getFileDate(this.sourceFile)).thenAnswer(CALLS_REAL_METHODS);

                expectedSchema = Schema.builder()
                        .addStringField(ReadFileStep.FILE_NAME_FIELD)
                        .addNullableStringField("meta_filedate")
                        .addDateTimeField("meta_load_timestamp")
                        .addNullableStringField("meta_ministry")
                        .addNullableStringField("meta_ministryid")
                        .addNullableStringField("field1")
                        .addNullableStringField("field2")
                        .addNullableStringField("field3")
                        .setOptions(Schema.Options.builder()
                                .setOption(ReadFileStep.OUTPUT_TABLE_FIELD, Schema.FieldType.STRING, "default")
                                .setOption(ReadFileStep.FILE_NAME_FIELD, Schema.FieldType.STRING, this.sourceFile)
                                .setOption("meta_filedate", Schema.FieldType.STRING.withNullable(true), "20230101")
                                .setOption("meta_load_timestamp", Schema.FieldType.DATETIME, TESTING_DT)
                                .setOption("meta_ministry", Schema.FieldType.STRING.withNullable(true), "MIN_VAL")
                                .setOption("meta_ministryid", Schema.FieldType.STRING.withNullable(true), "12345").build())
                        .build();

                expectedRows = List.of(
                        Row.withSchema(expectedSchema).addValues(this.sourceFile, "20230101", TESTING_DT, "MIN_VAL", "12345", "A", "B", "1").build(),
                        Row.withSchema(expectedSchema).addValues(this.sourceFile, "20230101", TESTING_DT, "MIN_VAL", "12345", "C", "D", "2").build(),
                        Row.withSchema(expectedSchema).addValues(this.sourceFile, "20230101", TESTING_DT, "MIN_VAL", "12345", "D", "E", "3").build());

            } else if (this.confFile.equals("csv-exception-athena-parse-error.conf")) {
                this.utilsMock.when(() -> Utils.readFromGCS(any())).thenReturn(ValueProvider.StaticValueProvider.of("{\"12345\" \"MIN_VAL\"}"));

            } else if (this.confFile.equals("csv-exception-athena-bad-filename.conf")) {
                this.utilsMock.when(() -> Utils.readFromGCS(any())).thenReturn(ValueProvider.StaticValueProvider.of("{\"12345\" \"MIN_VAL\"}"));

                athenaDnFnMockedStatic.when(() -> AthenaMetadataDoFn.getMinistryID(this.sourceFile)).thenAnswer(CALLS_REAL_METHODS);
                this.utilsMock.when(() -> Utils.readFromGCS(any())).thenReturn(ValueProvider.StaticValueProvider.of("{\"12345\": \"MIN_VAL\"}"));
                athenaDnFnMockedStatic.when(() -> AthenaMetadataDoFn.getFileDate(this.sourceFile)).thenAnswer(CALLS_REAL_METHODS);

                expectedSchema = Schema.builder()
                        .addStringField(ReadFileStep.FILE_NAME_FIELD)
                        .addNullableStringField("meta_filedate")
                        .addDateTimeField("meta_load_timestamp")
                        .addNullableStringField("meta_ministry")
                        .addNullableStringField("meta_ministryid")
                        .addNullableStringField("field1")
                        .addNullableStringField("field2")
                        .addNullableStringField("field3")
                        .setOptions(Schema.Options.builder()
                                .setOption(ReadFileStep.OUTPUT_TABLE_FIELD, Schema.FieldType.STRING, "default")
                                .setOption(ReadFileStep.FILE_NAME_FIELD, Schema.FieldType.STRING, this.sourceFile)
                                .setOption("meta_filedate", Schema.FieldType.STRING.withNullable(true), null)
                                .setOption("meta_load_timestamp", Schema.FieldType.DATETIME, TESTING_DT)
                                .setOption("meta_ministry", Schema.FieldType.STRING.withNullable(true), null)
                                .setOption("meta_ministryid", Schema.FieldType.STRING.withNullable(true), null).build())
                        .build();

                expectedRows = List.of(
                        Row.withSchema(expectedSchema).addValues(this.sourceFile, null, TESTING_DT, null, null, "A", "B", "1").build(),
                        Row.withSchema(expectedSchema).addValues(this.sourceFile, null, TESTING_DT, null, null, "C", "D", "2").build(),
                        Row.withSchema(expectedSchema).addValues(this.sourceFile, null, TESTING_DT, null, null, "D", "E", "3").build());

            }

            var config = ConfigBeanFactory.create(ConfigFactory.parseString(BaseTest.readConfAsString("ingestion/" + this.confFile)).resolve(), IngestionConfig.class);

            TransformerStep stepConfig;

            if (!this.confFile.contains("athena")) {
                stepConfig = (TransformerStep) config.getSteps().get(1);
            } else {
                stepConfig = (TransformerStep) config.getSteps().get(2);
            }

            MetadataDoFn.LOAD_TIMESTAMP = TESTING_DT;

            var gm = this.pipeline.apply(Create.of(inputRows));
            var bm = this.pipeline.apply(Create.of(List.<Row>of()).withCoder(RowCoder.of(inputSchema)));
            var pct = spy(PCollectionTuple.of(this.recordsTag, gm).and(this.badRecordsTag, bm));

            if (this.confFile.equals("csv-exception-athena-parse-error.conf")) {
                this.pipeline.enableAbandonedNodeEnforcement(false);
                var athenaStepConfig = (TransformerStep) config.getSteps().get(1);
                var result = pct.apply(stepConfig.getType(), Transformer.of(stepConfig, this.recordsTag, this.badRecordsTag, this.schemaTag, this.badSchemaTag));
                var exception = assertThrows(BaseStep.InvalidImplementationException.class, () -> result.apply(athenaStepConfig.getType(), Transformer.of(athenaStepConfig, this.recordsTag, this.badRecordsTag, this.schemaTag, this.badSchemaTag)));
                assertEquals("Error parsing json file: Unexpected character ('\"' (code 34)): was expecting a colon to separate field name and value\n" +
                        " at [Source: REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled); line: 1, column: 11]", exception.getCause().getCause().getMessage());

            } else {

                PCollectionTuple result;

                if (this.confFile.contains("athena")) {
                    var athenaStepConfig = (TransformerStep) config.getSteps().get(1);
                    result = pct.apply(athenaStepConfig.getType(), Transformer.of(athenaStepConfig, this.recordsTag, this.badRecordsTag, this.schemaTag, this.badSchemaTag));
                    result = result.apply(stepConfig.getType(), Transformer.of(stepConfig, this.recordsTag, this.badRecordsTag, this.schemaTag, this.badSchemaTag));
                } else {
                    result = pct.apply(stepConfig.getType(), Transformer.of(stepConfig, this.recordsTag, this.badRecordsTag, this.schemaTag, this.badSchemaTag));
                }

                // check good records against expected
                PAssert.that(result.get(this.recordsTag)).containsInAnyOrder(expectedRows);

                // for csv files, the reader will never return bad records
                PAssert.that(result.get(this.badRecordsTag)).containsInAnyOrder(List.of());

                this.pipeline.run();
            }
        }
    }
}
