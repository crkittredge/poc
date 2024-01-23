package org.ascension.addg.gcp.ingestion.read.file;

import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.Row;
import org.ascension.addg.gcp.BaseTest;
import org.ascension.addg.gcp.ingestion.*;
import org.ascension.addg.gcp.ingestion.core.IngestionConfig;
import org.ascension.addg.gcp.ingestion.read.Reader;
import org.ascension.addg.gcp.ingestion.core.BaseStep;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.*;
import static org.junit.runners.Parameterized.Parameters;

/**
 * Tests row-level scenarios for CSV files
 */
@RunWith(Parameterized.class)
public class ReadCSVDoFnTest extends IngestionTest.Setup {

    private final String confFile;
    private final List<Row> expectedRows;

    /**
     * Test parameters
     * @return parameter collection
     */
    @Parameters
    public static Collection<Object[]> data() {
        return List.of(new Object[][]{
                { "csv-positive.conf", "csv-with-header.csv",
                        Schema.builder()
                                .addNullableStringField("field1")
                                .addNullableStringField("field2")
                                .addNullableStringField("field3")
                                .setOptions(Schema.Options.builder()
                                        .setOption(ReadCSVStep.FILE_NAME_FIELD, Schema.FieldType.STRING, "csv-with-header.csv").build())
                                .build()},
                { "csv-positive-dynamic-dest.conf", "csv-with-header.csv",
                        Schema.builder()
                                .addNullableStringField("field1")
                                .addNullableStringField("field2")
                                .addNullableStringField("field3")
                                .setOptions(Schema.Options.builder()
                                        .setOption(ReadCSVStep.OUTPUT_TABLE_FIELD, Schema.FieldType.STRING, "prefix_with_header_suffix")
                                        .setOption(ReadCSVStep.FILE_NAME_FIELD, Schema.FieldType.STRING, "csv-with-header.csv").build())
                                .build()},
                { "csv-positive-headerinfo.conf", "csv-without-header.csv",
                        Schema.builder()
                                .addNullableStringField("field_1")
                                .addNullableStringField("field_2")
                                .addNullableStringField("field_3")
                                .setOptions(Schema.Options.builder()
                                        .setOption(ReadCSVStep.OUTPUT_TABLE_FIELD, Schema.FieldType.STRING, "csv_without_header_csv")
                                        .setOption(ReadCSVStep.FILE_NAME_FIELD, Schema.FieldType.STRING, "csv-without-header.csv").build())
                                .build()},
                { "csv-positive-generic-header.conf", "csv-without-header.csv",
                        Schema.builder()
                                .addNullableStringField("column_1")
                                .addNullableStringField("column_2")
                                .addNullableStringField("column_3")
                                .setOptions(Schema.Options.builder()
                                        .setOption(ReadCSVStep.FILE_NAME_FIELD, Schema.FieldType.STRING, "csv-without-header.csv").build())
                                .build()},
                { "csv-positive-empty-header-info.conf", "csv-without-header.csv",
                        Schema.builder()
                                .addNullableStringField("column_1")
                                .addNullableStringField("column_2")
                                .addNullableStringField("column_3")
                                .setOptions(Schema.Options.builder()
                                        .setOption(ReadCSVStep.FILE_NAME_FIELD, Schema.FieldType.STRING, "csv-without-header.csv").build())
                                .build()},
                { "csv-positive-custom-header.conf", "csv-without-header.csv",
                        Schema.builder()
                                .addNullableStringField("field_1")
                                .addNullableStringField("field_2")
                                .addNullableStringField("field_3")
                                .setOptions(Schema.Options.builder()
                                        .setOption(ReadCSVStep.FILE_NAME_FIELD, Schema.FieldType.STRING, "csv-without-header.csv").build())
                                .build() },
                { "csv-positive-no-header-info.conf", "csv-without-header.csv",
                        Schema.builder()
                                .addNullableStringField("column1")
                                .addNullableStringField("column2")
                                .addNullableStringField("column3")
                                .setOptions(Schema.Options.builder()
                                        .setOption(ReadCSVStep.FILE_NAME_FIELD, Schema.FieldType.STRING, "csv-without-header.csv").build())
                                .build() },
                { "csv-positive-tsv.conf", "csv-with-header.tsv",
                        Schema.builder()
                                .addNullableStringField("field1")
                                .addNullableStringField("field2")
                                .addNullableStringField("field3")
                                .setOptions(Schema.Options.builder()
                                        .setOption(ReadCSVStep.FILE_NAME_FIELD, Schema.FieldType.STRING, "csv-with-header.tsv").build())
                                .build() },
                { "csv-positive-headerinfo-no-match.conf", "csv-without-header.csv",
                        Schema.builder()
                                .addNullableStringField("column_1")
                                .addNullableStringField("column_2")
                                .addNullableStringField("column_3")
                                .setOptions(Schema.Options.builder()
                                        .setOption(ReadCSVStep.FILE_NAME_FIELD, Schema.FieldType.STRING, "csv-without-header.csv").build())
                                .build() },
                { "csv-positive-empty-csv-args.conf", "csv-with-header.csv",
                        Schema.builder()
                                .addNullableStringField("field1")
                                .addNullableStringField("field2")
                                .addNullableStringField("field3")
                                .setOptions(Schema.Options.builder()
                                        .setOption(ReadCSVStep.FILE_NAME_FIELD, Schema.FieldType.STRING, "csv-with-header.csv").build())
                                .build() },
                { "csv-exception-bad-class-impl.conf", "csv-with-header.csv",
                        Schema.builder()
                                .addNullableStringField("field1")
                                .addNullableStringField("field2")
                                .addNullableStringField("field3")
                                .setOptions(Schema.Options.builder()
                                        .setOption(ReadCSVStep.FILE_NAME_FIELD, Schema.FieldType.STRING, "csv-with-header.csv").build())
                                .build() },
        });
    }

    /**
     * Set up the test configuration
     * @param confFile configuration file name
     * @param sourceFile source file name
     * @param expectedSchema expected schema for the test
     * @throws URISyntaxException for errors when reading the config file
     */
    public ReadCSVDoFnTest(String confFile, String sourceFile, Schema expectedSchema) throws URISyntaxException {
        super(BaseTest.PIPELINE_ARGS, FileIngestionOptions.class, ReadFileDoFn.class);
        this.confFile = confFile;
        this.expectedRows = new ArrayList<>();

        var inputFile = Paths.get(Objects.requireNonNull(ReadCSVDoFnTest.class.getClassLoader().
                getResource("ingestion/" + sourceFile)).toURI()).toFile().getAbsolutePath();

        var options = (FileIngestionOptions) this.pipelineOptions;
        options.setInputFilePattern(ValueProvider.StaticValueProvider.of(inputFile));
        options.setPatternsFromFile(ValueProvider.StaticValueProvider.of(null));

        this.expectedRows.add(Row.withSchema(expectedSchema).addValues("A", "B", null).build());
        this.expectedRows.add(Row.withSchema(expectedSchema).addValues("C", "D", "2").build());
        this.expectedRows.add(Row.withSchema(expectedSchema).addValues("D", "E", "3").build());

    }

    @Override
    public void executeTest() {
        var config = ConfigBeanFactory.create(ConfigFactory.parseString(BaseTest.readConfAsString("ingestion/" + this.confFile)).resolve(), IngestionConfig.class);
        var stepConfig = (ReadFileStep) config.getSteps().get(0);

        try {
            var reader = Reader.of(stepConfig, this.pipelineOptions);

            if (this.confFile.equals("csv-exception-bad-class-impl.conf")) {
                this.pipeline.enableAbandonedNodeEnforcement(false);
                var exception = assertThrows(BaseStep.InvalidImplementationException.class, () -> this.pipeline.apply("ReadSource", reader));
                assertEquals("java.lang.ClassNotFoundException: some.bad.Class", exception.getMessage());

            } else {
                var result = this.pipeline.apply("ReadSource", reader);
                // check good records against expected
                PAssert.that(result.get(reader.getRecordsTag())).containsInAnyOrder(this.expectedRows);

                // for csv files, the reader will never return bad records
                PAssert.that(result.get(reader.getBadRecordsTag())).containsInAnyOrder(List.of());

                this.pipeline.run();
            }
        } catch (ReflectiveOperationException e) {
            fail(e.getMessage());
        }
    }
}
