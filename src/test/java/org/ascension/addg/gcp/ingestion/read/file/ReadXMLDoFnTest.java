package org.ascension.addg.gcp.ingestion.read.file;

import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.Row;
import org.ascension.addg.gcp.BaseTest;
import org.ascension.addg.gcp.ingestion.IngestionTest;
import org.ascension.addg.gcp.ingestion.core.IngestionConfig;
import org.ascension.addg.gcp.ingestion.core.Utils;
import org.ascension.addg.gcp.ingestion.read.ReadStep;
import org.ascension.addg.gcp.ingestion.read.Reader;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.Parameters;

/**
 * Tests row-level scenarios for XML files
 */
@RunWith(Parameterized.class)
public class ReadXMLDoFnTest extends IngestionTest.Setup {

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
                { "xml-positive.conf", "xml-sample.xml" },
                { "xml-positive.conf", "xml-sample-bad.xml" },
                { "xml-exception-dynamic-dest.conf", "xml-sample-bad.xml" }
        });
    }

    /**
     * Set up the test configuration
     * @param confFile configuration file name
     * @param sourceFile source file name
     * @throws URISyntaxException for errors when reading the config file
     */
    public ReadXMLDoFnTest(String confFile, String sourceFile) throws URISyntaxException {
        super(BaseTest.PIPELINE_ARGS, FileIngestionOptions.class, ReadFileDoFn.class);
        this.confFile = confFile;

        var inputFile = Paths.get(Objects.requireNonNull(ReadXMLDoFnTest.class.getClassLoader().
                getResource("ingestion/" + sourceFile)).toURI()).toFile().getAbsolutePath();

        var options = (FileIngestionOptions) this.pipelineOptions;
        options.setInputFilePattern(ValueProvider.StaticValueProvider.of(inputFile));
        options.setPatternsFromFile(ValueProvider.StaticValueProvider.of(null));

        if (sourceFile.equals("xml-sample.xml")) {

            var sob = Schema.Options.builder();

            if (!this.confFile.equals("xml-positive.conf")) {
                sob = sob.setOption(ReadStep.OUTPUT_TABLE_FIELD, Schema.FieldType.STRING, "prefix_json_sample_suffix");
            }

            sob = sob.setOption(ReadFileStep.FILE_NAME_FIELD, Schema.FieldType.STRING, sourceFile);

            var expectedSchema = Schema.builder()
                    .addNullableStringField("field_1")
                    .addNullableStringField("field_2")
                    .addNullableStringField("field_3")
                    .addNullableMapField("field_4", Schema.FieldType.STRING.withNullable(false), Schema.FieldType.array(Schema.FieldType.STRING.withNullable(true)).withNullable(true))
                    .addNullableMapField("field_5", Schema.FieldType.STRING.withNullable(false), Schema.FieldType.STRING.withNullable(true))
                    .setOptions(sob.build())
                    .build();

            this.expectedRows = List.of(
                    Row.withSchema(expectedSchema).addValues("A", "B", "1", Map.of("value", List.of("1", "2", "3")), Map.of("nested_2", "1", "nested_1", "A1")).build(),
                    Row.withSchema(expectedSchema).addValues("C", "D", "2", Map.of("value", List.of("1", "2", "3")), Map.of("nested_2", "2", "nested_1", "B2")).build(),
                    Row.withSchema(expectedSchema).addValues("E", "F", "3", Map.of("value", List.of("1", "2", "3")), Map.of("nested_2", "3", "nested_1", "C3")).build());

            this.expectedBadRows = List.of();

        } else {

            var sob = Schema.Options.builder();

            if (!this.confFile.equals("xml-positive.conf")) {
                sob = sob.setOption(ReadStep.OUTPUT_TABLE_FIELD, Schema.FieldType.STRING, "prefix_xml_sample_bad_suffix_error");
            }

            sob = sob.setOption(ReadFileStep.FILE_NAME_FIELD, Schema.FieldType.STRING, sourceFile);

            this.expectedRows = List.of();

            this.expectedBadRows = List.of(
                    Row.withSchema(Utils.getErrorSchema(sob.build()))
                            .addValues("Unexpected close tag </record>; expected </BAD>.\n" +
                                            " at [row,col {unknown-source}]: [5,141]\n" +
                                            " at [Source: (byte[])\"<root>\n" +
                                            "    <record><FIELD_1>A</FIELD_1><FIELD_2>B</FIELD_2><FIELD_3>1</FIELD_3><FIELD_4><value>1</value><value>2</value><value>3</value></FIELD_4><FIELD_5><NESTED_1>A1</NESTED_1><NESTED_2>1</NESTED_2></FIELD_5></record>\n" +
                                            "    <record><FIELD_1>C</FIELD_1><FIELD_2>D</FIELD_2><FIELD_3>2</FIELD_3><FIELD_4><value>1</value><value>2</value><value>3</value></FIELD_4><FIELD_5><NESTED_1>B2</NESTED_1><NESTED_2>2</NESTED_2></FIELD_5></record>\n" +
                                            "    <record><FIELD_1>E</FIELD_1><FIELD_2>F</FIELD_2><FIELD_3>3</FIE\"[truncated 296 bytes]; line: 5, column: 142]",
                                    "<root>\n" +
                                            "    <record><FIELD_1>A</FIELD_1><FIELD_2>B</FIELD_2><FIELD_3>1</FIELD_3><FIELD_4><value>1</value><value>2</value><value>3</value></FIELD_4><FIELD_5><NESTED_1>A1</NESTED_1><NESTED_2>1</NESTED_2></FIELD_5></record>\n" +
                                            "    <record><FIELD_1>C</FIELD_1><FIELD_2>D</FIELD_2><FIELD_3>2</FIELD_3><FIELD_4><value>1</value><value>2</value><value>3</value></FIELD_4><FIELD_5><NESTED_1>B2</NESTED_1><NESTED_2>2</NESTED_2></FIELD_5></record>\n" +
                                            "    <record><FIELD_1>E</FIELD_1><FIELD_2>F</FIELD_2><FIELD_3>3</FIELD_3><FIELD_4><value>1</value><value>2</value><value>3</value></FIELD_4><FIELD_5><NESTED_1>C3</NESTED_1><NESTED_2>3</NESTED_2></FIELD_5></record>\n" +
                                            "    <record><BAD>XML FIELD<FIELD_2>F</FIELD_2><FIELD_3>3</FIELD_3><FIELD_4><value>1</value><value>2</value><value>3</value></FIELD_4></record>\n" +
                                            "</root>").build());
        }
    }

    @Override
    public void executeTest() {
        var c = ConfigFactory.parseString(BaseTest.readConfAsString("ingestion/" + this.confFile)).resolve();
        var config = ConfigBeanFactory.create(c, IngestionConfig.class);
        var stepConfig = (ReadFileStep) config.getSteps().get(0);

        try {
            var reader = Reader.of(stepConfig, this.pipelineOptions);
            var result = this.pipeline.apply("ReadSource", reader);

            // check good records against expected
            PAssert.that(result.get(reader.getRecordsTag())).containsInAnyOrder(this.expectedRows);

            // for xml files, bad messages contain records with parsing errors
            PAssert.that(result.get(reader.getBadRecordsTag())).containsInAnyOrder(this.expectedBadRows);

            this.pipeline.run();
        } catch (ReflectiveOperationException e) {
            fail(e.getMessage());
        }
    }
}
