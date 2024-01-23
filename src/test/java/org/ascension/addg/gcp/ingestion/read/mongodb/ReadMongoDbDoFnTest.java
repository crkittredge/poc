package org.ascension.addg.gcp.ingestion.read.mongodb;

import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.ascension.addg.gcp.BaseTest;
import org.ascension.addg.gcp.ingestion.IngestionTest;
import org.ascension.addg.gcp.ingestion.core.IngestionConfig;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;
import org.ascension.addg.gcp.ingestion.core.Utils;
import org.ascension.addg.gcp.ingestion.read.Reader;
import org.bson.Document;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.Parameters;
import static org.mockito.Mockito.*;

/**
 * Tests row-level scenarios for reading data from MongoDb
 */
@RunWith(Parameterized.class)
public class ReadMongoDbDoFnTest extends IngestionTest.Setup {
    private final String confFile;
    private final List<Row> expectedRows;
    private final List<Row> expectedBadRows;
    private static final String[] RAW_MESSAGES = {
            "{ \"FIELD_1\": \"A\", \"FIELD_2\": \"B\", \"FIELD_3\": \"1\", \"FIELD_4\": [1, 2, 3], \"FIELD_5\": {\"NESTED_1\":  \"A1\", \"NESTED_2\":  1}, \"FIELD_6\": [], \"FIELD_7\": {} }",
            "{ \"FIELD_1\": \"C\", \"FIELD_2\": \"D\", \"FIELD_3\": \"2\", \"FIELD_4\": [1, 2, 3], \"FIELD_5\": {\"NESTED_1\":  \"B2\", \"NESTED_2\":  2}, \"FIELD_6\": [], \"FIELD_7\": {} }",
            "{ \"FIELD_1\": \"E\", \"FIELD_2\": \"F\", \"FIELD_3\": \"3\", \"FIELD_4\": [1, 2, 3], \"FIELD_5\": {\"NESTED_1\":  \"C3\", \"NESTED_2\":  3}, \"FIELD_6\": [], \"FIELD_7\": {} }"
    };

    /**
     * Test parameters
     * @return parameter collection
     */
    @Parameters
    public static Collection<Object[]> data() {
        return List.of(new Object[][]{
                { "mongodb-positive.conf" }
        });
    }

    /**
     * Set up the test configuration
     * @param confFile configuration file name
     */
    public ReadMongoDbDoFnTest(String confFile) {
        super(BaseTest.PIPELINE_ARGS, IngestionOptions.class, ReadMongoDbDoFn.class);
        this.confFile = confFile;

        this.expectedRows = List.of(
                Row.withSchema(Schema.builder()
                        .addNullableStringField("field_1")
                        .addNullableStringField("field_2")
                        .addNullableStringField("field_3")
                        .addNullableArrayField("field_4", Schema.FieldType.STRING.withNullable(true))
                        .addNullableMapField("field_5", Schema.FieldType.STRING.withNullable(false), Schema.FieldType.STRING.withNullable(true))
                        .addNullableArrayField("field_6", Schema.FieldType.STRING.withNullable(true))
                        .addNullableMapField("field_7", Schema.FieldType.STRING.withNullable(false), Schema.FieldType.STRING.withNullable(true))
                        .setOptions(Schema.Options.builder().build())
                        .build()).addValues("A", "B", "1", List.of("1", "2", "3"), Map.of("nested_2", "1", "nested_1", "A1"), List.of(), Map.of()).build(),
                Row.withSchema(Schema.builder()
                        .addNullableStringField("field_1")
                        .addNullableStringField("field_2")
                        .addNullableStringField("field_3")
                        .addNullableArrayField("field_4", Schema.FieldType.STRING.withNullable(true))
                        .addNullableMapField("field_5", Schema.FieldType.STRING.withNullable(false), Schema.FieldType.STRING.withNullable(true))
                        .addNullableArrayField("field_6", Schema.FieldType.STRING.withNullable(true))
                        .addNullableMapField("field_7", Schema.FieldType.STRING.withNullable(false), Schema.FieldType.STRING.withNullable(true))
                        .setOptions(Schema.Options.builder().build())
                        .build()).addValues("C", "D", "2", List.of("1", "2", "3"), Map.of("nested_2", "2", "nested_1", "B2"), List.of(), Map.of()).build(),
                Row.withSchema(Schema.builder()
                        .addNullableStringField("field_1")
                        .addNullableStringField("field_2")
                        .addNullableStringField("field_3")
                        .addNullableArrayField("field_4", Schema.FieldType.STRING.withNullable(true))
                        .addNullableMapField("field_5", Schema.FieldType.STRING.withNullable(false), Schema.FieldType.STRING.withNullable(true))
                        .addNullableArrayField("field_6", Schema.FieldType.STRING.withNullable(true))
                        .addNullableMapField("field_7", Schema.FieldType.STRING.withNullable(false), Schema.FieldType.STRING.withNullable(true))
                        .setOptions(Schema.Options.builder().build())
                        .build()).addValues("E", "F", "3", List.of("1", "2", "3"), Map.of("nested_2", "3", "nested_1", "C3"), List.of(), Map.of()).build());

        this.expectedBadRows = List.of();
    }

    private PCollection<Document> getDocumentPC() {
        var messages = new ArrayList<Document>();
        for (var i = 0; i < 3; i++) {
            messages.add(Document.parse(ReadMongoDbDoFnTest.RAW_MESSAGES[i]));
        }
        return this.pipeline.apply(Create.of(messages).withCoder(SerializableCoder.of(Document.class))).setCoder(SerializableCoder.of(Document.class));
    }

    @Override
    public void executeTest() {
        var config = ConfigBeanFactory.create(ConfigFactory.parseString(BaseTest.readConfAsString("ingestion/" + this.confFile)).resolve(), IngestionConfig.class);
        var stepConfig = (ReadMongoDbStep) config.getSteps().get(0);
        var pipelineSpy = spy(this.pipeline.begin());

        try (var mdbioStaticMock = mockStatic(MongoDbIO.class, CALLS_REAL_METHODS)) {
            var mdbioReadMock = spy(MongoDbIO.Read.class);

            doReturn(mdbioReadMock).when(mdbioReadMock).withUri(any(String.class));
            doReturn(mdbioReadMock).when(mdbioReadMock).withDatabase(any(String.class));
            doReturn(mdbioReadMock).when(mdbioReadMock).withCollection(any(String.class));
            doReturn(this.getDocumentPC()).when(pipelineSpy).apply(eq("Read from MongoDb"), any());
            var reader = Reader.of(stepConfig, this.pipelineOptions);
            mdbioStaticMock.when(MongoDbIO::read).thenReturn(mdbioReadMock);
            var result = pipelineSpy.apply("ReadSource", reader);

            // check good records against expected
            PAssert.that(result.get(reader.getRecordsTag())).containsInAnyOrder(this.expectedRows);

            // for mongodb we don't have bad records
            PAssert.that(result.get(reader.getBadRecordsTag())).containsInAnyOrder(this.expectedBadRows);

            this.pipeline.run();
        } catch (ReflectiveOperationException e) {
            fail(e.getMessage());
        }
    }
}
