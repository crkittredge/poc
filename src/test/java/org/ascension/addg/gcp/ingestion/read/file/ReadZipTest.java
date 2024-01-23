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
import org.ascension.addg.gcp.ingestion.core.Utils;
import org.ascension.addg.gcp.ingestion.read.ReadStep;
import org.ascension.addg.gcp.ingestion.read.Reader;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.fail;

/**
 * Tests reading from a ZIP archive
 */
@RunWith(Parameterized.class)
public class ReadZipTest extends IngestionTest.Setup {

    private final String confFile;
    private final List<Row> expectedRows;
    private final List<Row> expectedBadRows;

    /**
     * Test parameters
     * @return parameter collection
     */
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return List.of(new Object[][]{
                { "csv-positive-zip.conf", "csv-with-header.zip" },
                { "csv-positive-zip-dynamic-dest.conf", "csv-with-header.zip" },
                { "json-positive-zip.conf", "json-sample.zip" }
        });
    }

    /**
     * Set up the test configuration
     * @param confFile configuration file name
     * @param sourceFile source file name
     * @throws URISyntaxException for errors when reading the config file
     */
    public ReadZipTest(String confFile, String sourceFile) throws URISyntaxException {
        super(BaseTest.PIPELINE_ARGS, FileIngestionOptions.class, ReadFileDoFn.class);
        this.confFile = confFile;

        var inputFile = Paths.get(Objects.requireNonNull(ReadTarTest.class.getClassLoader().
                getResource("ingestion/" + sourceFile)).toURI()).toFile().getAbsolutePath();

        var options = (FileIngestionOptions) this.pipelineOptions;
        options.setInputFilePattern(ValueProvider.StaticValueProvider.of(inputFile));
        options.setPatternsFromFile(ValueProvider.StaticValueProvider.of(null));

        // json
        if (this.confFile.equals("json-positive-zip.conf")) {

            var expectedSchema = Schema.builder()
                    .addNullableStringField("field_1")
                    .addNullableStringField("field_2")
                    .addNullableStringField("field_3")
                    .addNullableArrayField("field_4", Schema.FieldType.STRING.withNullable(true))
                    .setOptions(Schema.Options.builder()
                            .setOption(ReadFileStep.ARCHIVE_FILE_NAME_FIELD, Schema.FieldType.STRING, sourceFile)
                            .setOption(ReadFileStep.FILE_NAME_FIELD, Schema.FieldType.STRING, "fileIngestionTest_json_2.json").build())
                    .build();

            this.expectedRows = List.of(
                    Row.withSchema(expectedSchema).addValues("A", "B", "1", List.of("1", "2", "3")).build(),
                    Row.withSchema(expectedSchema).addValues("C", "D", "2", List.of("1", "2", "3")).build(),
                    Row.withSchema(expectedSchema).addValues("E", "F", "3", List.of("1", "2", "3")).build());

            this.expectedBadRows = List.of(
                    Row.withSchema(Utils.getErrorSchema(Schema.Options.builder()
                                    .setOption(ReadFileStep.ARCHIVE_FILE_NAME_FIELD, Schema.FieldType.STRING, sourceFile)
                                    .setOption(ReadFileStep.FILE_NAME_FIELD, Schema.FieldType.STRING, "fileIngestionTest_json_2.json").build()))
                            .addValue("Unexpected character ('J' (code 74)): was expecting a colon to separate field name and value\n at [Source: REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled); line: 1, column: 11]")
                            .addValue("{ \"BAD: \"JSON FIELD\", \"FIELD_2\": \"F\", \"FIELD_3\": \"3\", \"FIELD_4\": [1, 2, 3] }").build());
        // csv
        } else {

            var sob = Schema.Options.builder();

            if (!this.confFile.equals("csv-positive-zip.conf")) {
                sob = sob.setOption(ReadStep.OUTPUT_TABLE_FIELD, Schema.FieldType.STRING, "prefix_fileingestiontest_csv_5_suffix");
            }

            sob = sob
                    .setOption(ReadCSVStep.ARCHIVE_FILE_NAME_FIELD, Schema.FieldType.STRING, sourceFile)
                    .setOption(ReadFileStep.FILE_NAME_FIELD, Schema.FieldType.STRING, "fileIngestionTest_csv_5.csv");

            var expectedSchema = Schema.builder()
                    .addNullableStringField("field1")
                    .addNullableStringField("field2")
                    .addNullableStringField("field3")
                    .setOptions(sob.build())
                    .build();

            this.expectedRows = List.of(
                    Row.withSchema(expectedSchema).addValues("A", "B", "1").build(),
                    Row.withSchema(expectedSchema).addValues("C", "D", "2").build(),
                    Row.withSchema(expectedSchema).addValues("D", "E", "3").build());

            this.expectedBadRows = List.of();
        }
    }

    @Override
    public void executeTest() {
        var config = ConfigBeanFactory.create(ConfigFactory.parseString(BaseTest.readConfAsString("ingestion/" + this.confFile)).resolve(), IngestionConfig.class);
        var stepConfig = (ReadFileStep) config.getSteps().get(0);

        try {
            var reader = Reader.of(stepConfig, this.pipelineOptions);
            var result = this.pipeline.apply("ReadSource", reader);

            // check good records against expected
            PAssert.that(result.get(reader.getRecordsTag())).containsInAnyOrder(this.expectedRows);

            // check bad records against expected
            PAssert.that(result.get(reader.getBadRecordsTag())).containsInAnyOrder(this.expectedBadRows);

            this.pipeline.run();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
