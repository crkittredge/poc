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
import org.ascension.addg.gcp.ingestion.transform.Transformer;
import org.ascension.addg.gcp.ingestion.transform.TransformerStep;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.Parameters;

/**
 * Tests row-level scenarios for JSON files
 */
@RunWith(Parameterized.class)
public class ReadJSONDoFnTest extends IngestionTest.Setup {

    private final String confFile;
    private final List<Row> expectedRows;
    private final List<Row> expectedFlattenedRows;
    private final List<Row> expectedBadRows;

    /**
     * Test parameters
     * @return parameter collection
     */
    @Parameters
    public static Collection<Object[]> data() {
        return List.of(new Object[][]{
                { "json-positive.conf", "json-sample.json" },
                { "json-positive-dynamic-dest.conf", "json-sample.json" }
        });
    }

    /**
     * Set up the test configuration
     * @param confFile configuration file name
     * @param sourceFile source file name
     * @throws URISyntaxException for errors when reading the config file
     */
    public ReadJSONDoFnTest(String confFile, String sourceFile) throws URISyntaxException {
        super(BaseTest.PIPELINE_ARGS, FileIngestionOptions.class, ReadFileDoFn.class);
        this.confFile = confFile;

        var inputFile = Paths.get(Objects.requireNonNull(ReadJSONDoFnTest.class.getClassLoader().
                getResource("ingestion/" + sourceFile)).toURI()).toFile().getAbsolutePath();

        var options = (FileIngestionOptions) this.pipelineOptions;
        options.setInputFilePattern(ValueProvider.StaticValueProvider.of(inputFile));
        options.setPatternsFromFile(ValueProvider.StaticValueProvider.of(null));

        var sob = Schema.Options.builder();

        if (!this.confFile.equals("json-positive.conf")) {
            sob = sob.setOption(ReadStep.OUTPUT_TABLE_FIELD, Schema.FieldType.STRING, "prefix_json_sample_suffix");
        }

        sob = sob.setOption(ReadFileStep.FILE_NAME_FIELD, Schema.FieldType.STRING, sourceFile);

        var expectedSchema = Schema.builder()
                .addNullableStringField("field_1")
                .addNullableStringField("field_2")
                .addNullableStringField("field_3")
                .addNullableArrayField("field_4", Schema.FieldType.STRING.withNullable(true))
                .addNullableMapField("field_5", Schema.FieldType.STRING.withNullable(false), Schema.FieldType.STRING.withNullable(true))
                .addNullableArrayField("field_6", Schema.FieldType.STRING.withNullable(true))
                .addNullableMapField("field_7", Schema.FieldType.STRING.withNullable(false), Schema.FieldType.STRING.withNullable(true))
                .setOptions(sob.build())
                .build();

        this.expectedRows = List.of(
                Row.withSchema(expectedSchema).addValues("A", "B", "1", List.of("1", "2", "3"), Map.of("nested_2", "1", "nested_1", "A1"), List.of(), Map.of()).build(),
                Row.withSchema(expectedSchema).addValues("C", "D", "2", List.of("1", "2", "3"), Map.of("nested_2", "2", "nested_1", "B2"), List.of(), Map.of()).build(),
                Row.withSchema(expectedSchema).addValues("E", "F", "3", List.of("1", "2", "3"), Map.of("nested_2", "3", "nested_1", "C3"), List.of(), Map.of()).build());

        var expectedFlattenedSchema = Schema.builder()
                .addNullableStringField("field_1")
                .addNullableStringField("field_2")
                .addNullableStringField("field_3")
                .addNullableStringField("field_4_1")
                .addNullableStringField("field_4_2")
                .addNullableStringField("field_4_3")
                .addNullableStringField("field_5_nested_2")
                .addNullableStringField("field_5_nested_1")
                .setOptions(sob.build())
                .build();

        this.expectedFlattenedRows = List.of(
                Row.withSchema(expectedFlattenedSchema).addValues("A", "B", "1", "1", "2", "3", "1", "A1").build(),
                Row.withSchema(expectedFlattenedSchema).addValues("C", "D", "2", "1", "2", "3", "2", "B2").build(),
                Row.withSchema(expectedFlattenedSchema).addValues("E", "F", "3", "1", "2", "3", "3", "C3").build());


        var sobBad = Schema.Options.builder();

        if (!this.confFile.equals("json-positive.conf")) {
            sobBad = sobBad.setOption(ReadStep.OUTPUT_TABLE_FIELD, Schema.FieldType.STRING, "prefix_json_sample_suffix_error");
        }

        sobBad = sobBad.setOption(ReadFileStep.FILE_NAME_FIELD, Schema.FieldType.STRING, sourceFile);

        this.expectedBadRows = List.of(
                Row.withSchema(Utils.getErrorSchema(sobBad.build()))
                        .addValues("Unexpected character ('J' (code 74)): was expecting a colon to separate field name and value\n at [Source: REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled); line: 1, column: 11]",
                                "{ \"BAD: \"JSON FIELD\", \"FIELD_2\": \"F\", \"FIELD_3\": \"3\", \"FIELD_4\": [1, 2, 3] }").build());
    }

    @Override
    public void executeTest() {
        var c = ConfigFactory.parseString(BaseTest.readConfAsString("ingestion/" + this.confFile)).resolve();
        var config = ConfigBeanFactory.create(c, IngestionConfig.class);
        var stepConfig = (ReadFileStep) config.getSteps().get(0);
        var flattenConfig = (TransformerStep) config.getSteps().get(1);

        try {
            var reader = Reader.of(stepConfig, this.pipelineOptions);
            var result = this.pipeline.apply("ReadSource", reader);

            // check good records against expected
            PAssert.that(result.get(reader.getRecordsTag())).containsInAnyOrder(this.expectedRows);

            // for json files, bad messages contain records with parsing errors
            PAssert.that(result.get(reader.getBadRecordsTag())).containsInAnyOrder(this.expectedBadRows);

            // flatten the nested columns
            result = result.apply("Flatten", Transformer.of(flattenConfig, reader.getRecordsTag(), reader.getBadRecordsTag(), reader.getSchemaTag(), reader.getBadSchemaTag()));

            // make sure the data was flattened correctly
            PAssert.that(result.get(reader.getRecordsTag())).containsInAnyOrder(this.expectedFlattenedRows);

            this.pipeline.run();
        } catch (ReflectiveOperationException e) {
            fail(e.getMessage());
        }
    }
}
