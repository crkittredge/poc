package org.ascension.addg.gcp.ingestion.write.avro;

import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.ascension.addg.gcp.BaseTest;
import org.ascension.addg.gcp.ingestion.IngestionTest;
import org.ascension.addg.gcp.ingestion.core.IngestionConfig;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;
import org.ascension.addg.gcp.ingestion.read.ReadStep;
import org.ascension.addg.gcp.ingestion.transform.Transformer;
import org.ascension.addg.gcp.ingestion.transform.metadata.MetadataDoFn;
import org.ascension.addg.gcp.ingestion.transform.metadata.MetadataStep;
import org.ascension.addg.gcp.ingestion.write.Writer;
import org.joda.time.Instant;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentMatchers;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.Parameters;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class AvroWriterTest extends IngestionTest.Setup {
    private final String confFile;
    private final PCollectionTuple inputData;
    private final List<Row> expectedRows = new ArrayList<>();

    /**
     * Test parameters
     * @return parameter collection
     */
    @Parameters
    public static Collection<Object[]> data() {
        return List.of(new Object[][]{
                { "avro-positive-write.conf" }
        });
    }

    /**
     * Set up the test configuration
     * @param confFile configuration file name
     */
    public AvroWriterTest(String confFile) {
        super(BaseTest.PIPELINE_ARGS, IngestionOptions.class, AvroWriter.class);

        this.confFile = confFile;
        var inputRows = new ArrayList<Row>();
        var inputBadRows = new ArrayList<Row>();

        final Instant TESTING_DT = Instant.parse("2023-11-24T00:00:00.000Z");
        MetadataDoFn.LOAD_TIMESTAMP = TESTING_DT;

        this.pipelineOptions.setBqLandProject(null);
        this.pipelineOptions.setBqLandDataset(null);

        var inputSchema = Schema.builder()
                .addNullableInt32Field("id")
                .addNullableStringField("name")
                .addNullableBooleanField("boolean_value")
                .addNullableFloatField("float_value")
                .addNullableInt64Field("long_value")
                .addNullableDateTimeField("timestamp_value")
                .build();

        for (int i = 0; i < 20; i++) {
            inputRows.add(Row.withSchema(inputSchema)
                    .addValues(i, "value_" + i, true, 5.5F, i * 800000L,
                            Instant.ofEpochMilli(LocalDateTime.of(2023, 1, 2, 12, 5, 0).toInstant(ZoneOffset.UTC).toEpochMilli()))
                    .build());
        }

        var inputBadSchema = Schema.builder()
                .addNullableInt32Field("id")
                .addNullableStringField("name")
                .addNullableBooleanField("boolean_value")
                .addNullableFloatField("float_value")
                .addNullableInt64Field("long_value")
                .addNullableDateTimeField("timestamp_value")
                .build();

        for (int i = 0; i < 20; i++) {
            inputBadRows.add(Row.withSchema(inputBadSchema)
                    .addValues(i, "value_" + i, true, 5.5F, i * 800000L,
                            Instant.ofEpochMilli(LocalDateTime.of(2023, 1, 2, 12, 5, 0).toInstant(ZoneOffset.UTC).toEpochMilli()))
                    .build());
        }

        this.inputData = PCollectionTuple.of(this.recordsTag, this.pipeline.apply(Create.of(inputRows))).and(this.badRecordsTag, this.pipeline.apply(Create.of(inputBadRows)));

        var expectedSchema = Schema.builder()
                .addDateTimeField("meta_load_timestamp")
                .addNullableInt32Field("id")
                .addNullableStringField("name")
                .addNullableBooleanField("boolean_value")
                .addNullableFloatField("float_value")
                .addNullableInt64Field("long_value")
                .addNullableDateTimeField("timestamp_value")
                .build();

        for (int i = 0; i < 20; i++) {
            expectedRows.add(Row.withSchema(expectedSchema)
                    .addValues(TESTING_DT, i, "value_" + i, true, 5.5F, i * 800000L,
                            Instant.ofEpochMilli(LocalDateTime.of(2023, 1, 2, 12, 5, 0).toInstant(ZoneOffset.UTC).toEpochMilli()))
                    .build());
        }
    }

    @Override
    public void executeTest() {
        var config = ConfigBeanFactory.create(ConfigFactory.parseString(BaseTest.readConfAsString("ingestion/" + this.confFile)).resolve(), IngestionConfig.class);
        var metadataConfig = (MetadataStep) config.getSteps().get(1);
        var writeConfig = (WriteAvroStep) config.getSteps().get(2);

        var fio = FileIO.<String, Row>writeDynamic();
        var fioWriteSpy = spy(fio);

        try (var fioStaticMock = mockStatic(FileIO.class, CALLS_REAL_METHODS)){
            fioStaticMock.when(FileIO::writeDynamic).thenReturn(fioWriteSpy);

            // write PCollection of Row to avro
            this.inputData
                    .apply("InjectMetadata", Transformer.of(metadataConfig, this.recordsTag, this.badRecordsTag, this.schemaTag, this.badSchemaTag))
                    .apply("WriteAvroTest", Writer.of(writeConfig, this.pipelineOptions, this.recordsTag, this.badRecordsTag, this.schemaTag, this.badSchemaTag));

            this.pipeline.run().waitUntilFinish();

            // read back into PCollection of Row
            var pc = this.pipeline
                    .apply("File List", Create.of(List.of(writeConfig.getOutputRoot() + "/table=" + writeConfig.getOutputDirectory() + "/meta_load_date=*/*" + writeConfig.getFilenameSuffix())))
                    .apply("Match Input Files", FileIO.matchAll())
                    .apply("Read Matched Files", FileIO.readMatches())
                    .apply("Parse Rows", AvroIO.parseFilesGenericRecords(r -> AvroUtils.toBeamRowStrict(r, AvroUtils.toBeamSchema(r.getSchema()))).withCoder(SerializableCoder.of(Row.class)));

            var pcBad = this.pipeline
                    .apply("File List", Create.of(List.of(writeConfig.getOutputRoot() + "/table=" + writeConfig.getOutputDirectory() + writeConfig.getErrorDirectorySuffix() + "/meta_load_date=*/*" + writeConfig.getFilenameSuffix())))
                    .apply("Match Input Files", FileIO.matchAll())
                    .apply("Read Matched Files", FileIO.readMatches())
                    .apply("Parse Rows", AvroIO.parseFilesGenericRecords(r -> AvroUtils.toBeamRowStrict(r, AvroUtils.toBeamSchema(r.getSchema()))).withCoder(SerializableCoder.of(Row.class)));

            // verify we read back what we initially wrote
            PAssert.that(pc).containsInAnyOrder(expectedRows);
            PAssert.that(pcBad).containsInAnyOrder(expectedRows);

            this.pipeline.run();

            verify(fioWriteSpy, times(2)).by(ArgumentMatchers.<SerializableFunction<Row, String>>any());
            verify(fioWriteSpy, times(2)).by(ArgumentMatchers.<Contextful<Contextful.Fn<Row, String>>>any());

            assertEquals(".avro", writeConfig.getFilenameSuffix());
            assertEquals("./.avro", writeConfig.getOutputRoot());
            assertEquals("test", writeConfig.getOutputDirectory());
            assertEquals("_error", writeConfig.getErrorDirectorySuffix());
            assertEquals(Integer.valueOf(1), writeConfig.getNumShards());

        } catch (ReflectiveOperationException e) {
            fail(e.getMessage());

        //  clean up avro files
        } finally {
            try (var stream = Files.walk(Paths.get(writeConfig.getOutputRoot()))) {
                stream.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            } catch (IOException e) {
                //do nothing
            }
        }
    }
}
