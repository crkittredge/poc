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
import org.ascension.addg.gcp.ingestion.read.ReadStep;
import org.ascension.addg.gcp.ingestion.read.Reader;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.Parameters;

/**
 * Tests reading from a TAR archive
 */
@RunWith(Parameterized.class)
public class ReadTarTest extends IngestionTest.Setup {

    private final String confFile;
    private final List<Row> expectedRows;

    /**
     * Test parameters
     * @return parameter collection
     */
    @Parameters
    public static Collection<Object[]> data() {
        return List.of(new Object[][]{
                { "csv-positive-tar.conf", "csv-with-header.tar.gz" },
                { "csv-positive-tar-dynamic-dest.conf", "csv-with-header.tar.gz" }
        });
    }

    /**
     * Set up the test configuration
     * @param confFile configuration file name
     * @param sourceFile source file name
     * @throws URISyntaxException for errors when reading the config file
     */
    public ReadTarTest(String confFile, String sourceFile) throws URISyntaxException {
        super(BaseTest.PIPELINE_ARGS, FileIngestionOptions.class, ReadFileDoFn.class);
        this.confFile = confFile;

        var inputFile = Paths.get(Objects.requireNonNull(ReadTarTest.class.getClassLoader().
                getResource("ingestion/" + sourceFile)).toURI()).toFile().getAbsolutePath();

        var options = (FileIngestionOptions) this.pipelineOptions;
        options.setInputFilePattern(ValueProvider.StaticValueProvider.of(inputFile));
        options.setPatternsFromFile(ValueProvider.StaticValueProvider.of(null));

        var sob = Schema.Options.builder();

        if (!this.confFile.equals("csv-positive-tar.conf")) {
            sob = sob.setOption(ReadStep.OUTPUT_TABLE_FIELD, Schema.FieldType.STRING, "prefix_fileingestiontest_csv_6_suffix");
        }

        sob = sob
                .setOption(ReadCSVStep.ARCHIVE_FILE_NAME_FIELD, Schema.FieldType.STRING, sourceFile)
                .setOption(ReadFileStep.FILE_NAME_FIELD, Schema.FieldType.STRING, "fileIngestionTest_csv_6.csv");

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

            // for csv files, the reader will never return bad records
            PAssert.that(result.get(reader.getBadRecordsTag())).containsInAnyOrder(List.of());

            this.pipeline.run();
        } catch (ReflectiveOperationException e) {
            fail(e.getMessage());
        }
    }
}
