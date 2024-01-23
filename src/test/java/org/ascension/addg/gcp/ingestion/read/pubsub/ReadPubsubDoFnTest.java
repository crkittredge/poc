package org.ascension.addg.gcp.ingestion.read.pubsub;

import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesAndMessageIdCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.ascension.addg.gcp.BaseTest;
import org.ascension.addg.gcp.ingestion.*;
import org.ascension.addg.gcp.ingestion.core.IngestionConfig;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;
import org.ascension.addg.gcp.ingestion.core.Utils;
import org.ascension.addg.gcp.ingestion.read.Reader;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.junit.runners.Parameterized.Parameters;
import static org.mockito.Mockito.*;

/**
 * Tests row-level scenarios for reading data from PubSub
 */
@RunWith(Parameterized.class)
public class ReadPubsubDoFnTest extends IngestionTest.Setup {
    private final String confFile;
    private final List<Row> expectedRows;
    private final List<Row> expectedBadRows;
    private static final String[] RAW_MESSAGES = {
            "{ \"FIELD_1\": \"A\", \"FIELD_2\": \"B\", \"FIELD_3\": \"1\", \"FIELD_4\": [1, 2, 3], \"FIELD_5\": {\"NESTED_1\":  \"A1\", \"NESTED_2\":  1} }",
            "{ \"FIELD_1\": \"C\", \"FIELD_2\": \"D\", \"FIELD_3\": \"2\", \"FIELD_4\": [1, 2, 3], \"FIELD_5\": {\"NESTED_1\":  \"B2\", \"NESTED_2\":  2} }",
            "{ \"FIELD_1\": \"E\", \"FIELD_2\": \"F\", \"FIELD_3\": \"3\", \"FIELD_4\": [1, 2, 3], \"FIELD_5\": {\"NESTED_1\":  \"C3\", \"NESTED_2\":  3} }",
            "{ \"BAD: \"JSON FIELD\", \"FIELD_2\": \"F\", \"FIELD_3\": \"3\", \"FIELD_4\": [1, 2, 3] }"
    };

    /**
     * Test parameters
     * @return parameter collection
     */
    @Parameters
    public static Collection<Object[]> data() {
        return List.of(new Object[][]{
                { "pubsub-positive-subscription.conf" },
                { "pubsub-positive-topic.conf" },
                { "pubsub-exception-no-source.conf" }
        });
    }

    /**
     * Set up the test configuration
     * @param confFile configuration file name
     */
    public ReadPubsubDoFnTest(String confFile) {
        super(BaseTest.PIPELINE_ARGS, IngestionOptions.class, ReadPubsubDoFn.class);
        this.confFile = confFile;

        this.expectedRows = List.of(
                Row.withSchema(Schema.builder()
                        .addNullableStringField("field_1")
                        .addNullableStringField("field_2")
                        .addNullableStringField("field_3")
                        .addNullableArrayField("field_4", Schema.FieldType.STRING.withNullable(true))
                        .addNullableMapField("field_5", Schema.FieldType.STRING.withNullable(false), Schema.FieldType.STRING.withNullable(true))
                        .setOptions(Schema.Options.builder()
                                .setOption("meta_pubsub_message_id", Schema.FieldType.STRING, "0")
                                .setOption("meta_pubsub_attributes", Schema.FieldType.STRING, "key1=val1,key2=val2").build())
                        .build()).addValues("A", "B", "1", List.of("1", "2", "3"), Map.of("nested_1", "A1", "nested_2", "1")).build(),
                Row.withSchema(Schema.builder()
                        .addNullableStringField("field_1")
                        .addNullableStringField("field_2")
                        .addNullableStringField("field_3")
                        .addNullableArrayField("field_4", Schema.FieldType.STRING.withNullable(true))
                        .addNullableMapField("field_5", Schema.FieldType.STRING.withNullable(false), Schema.FieldType.STRING.withNullable(true))
                        .setOptions(Schema.Options.builder()
                                .setOption("meta_pubsub_message_id", Schema.FieldType.STRING, "1")
                                .setOption("meta_pubsub_attributes", Schema.FieldType.STRING, "key1=val1,key2=val2").build())
                        .build()).addValues("C", "D", "2", List.of("1", "2", "3"), Map.of("nested_2", "2", "nested_1", "B2")).build(),
                Row.withSchema(Schema.builder()
                        .addNullableStringField("field_1")
                        .addNullableStringField("field_2")
                        .addNullableStringField("field_3")
                        .addNullableArrayField("field_4", Schema.FieldType.STRING.withNullable(true))
                        .addNullableMapField("field_5", Schema.FieldType.STRING.withNullable(false), Schema.FieldType.STRING.withNullable(true))
                        .setOptions(Schema.Options.builder()
                                .setOption("meta_pubsub_message_id", Schema.FieldType.STRING, "2")
                                .setOption("meta_pubsub_attributes", Schema.FieldType.STRING, "key1=val1,key2=val2").build())
                        .build()).addValues("E", "F", "3", List.of("1", "2", "3"), Map.of("nested_2", "3", "nested_1", "C3")).build());

        this.expectedBadRows = List.of(
                Row.withSchema(Utils.getErrorSchema(Schema.Options.builder()
                                .setOption("meta_pubsub_message_id", Schema.FieldType.STRING, "3")
                                .setOption("meta_pubsub_attributes", Schema.FieldType.STRING, "key1=val1,key2=val2").build()))
                        .addValue("Unexpected character ('J' (code 74)): was expecting a colon to separate field name and value\n at " +
                                "[Source: REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled); line: 1, column: 11]")
                        .addValue(ReadPubsubDoFnTest.RAW_MESSAGES[3]).build());
    }

    private PCollection<PubsubMessage> getMessagePC() {
        var messages = new ArrayList<PubsubMessage>();
        for (var i = 0; i < 4; i++) {
            messages.add(new PubsubMessage(ReadPubsubDoFnTest.RAW_MESSAGES[i].getBytes(), Map.of("key1", "val1", "key2", "val2"), String.valueOf(i)));
        }
        return this.pipeline.apply(Create.of(messages).withCoder(PubsubMessageWithAttributesAndMessageIdCoder.of())).setCoder(PubsubMessageWithAttributesAndMessageIdCoder.of());
    }

    @Override
    public void executeTest() {
        var config = ConfigBeanFactory.create(ConfigFactory.parseString(BaseTest.readConfAsString("ingestion/" + this.confFile)).resolve(), IngestionConfig.class);
        var stepConfig = (ReadPubsubStep) config.getSteps().get(0);
        var pipelineSpy = spy(this.pipeline.begin());

        if (this.confFile.equals("pubsub-exception-no-source.conf")) {
            this.pipeline.enableAbandonedNodeEnforcement(false);
            var exception = assertThrows(InvocationTargetException.class, () -> Reader.of(stepConfig, this.pipelineOptions));
            assertEquals("One of topic or subscription are required", exception.getCause().getMessage());

        } else {
            try (var psioStaticMock = mockStatic(PubsubIO.class, CALLS_REAL_METHODS)) {
                var psioReadMock = spy(PubsubIO.Read.class);

                if (this.confFile.equals("pubsub-positive-subscription.conf")) {
                    doReturn(null).when(psioReadMock).fromSubscription(any(String.class));
                } else if (this.confFile.equals("pubsub-positive-topic.conf")) {
                    doReturn(null).when(psioReadMock).fromTopic(any(String.class));
                }
                doReturn(this.getMessagePC()).when(pipelineSpy).apply(eq("Read from Pub/Sub"), any());
                var reader = Reader.of(stepConfig, this.pipelineOptions);
                psioStaticMock.when(PubsubIO::readMessagesWithAttributesAndMessageId).thenReturn(psioReadMock);
                var result = pipelineSpy.apply("ReadSource", reader);

                // check good records against expected
                PAssert.that(result.get(reader.getRecordsTag())).containsInAnyOrder(this.expectedRows);

                // for pubsub we can have bad json records
                PAssert.that(result.get(reader.getBadRecordsTag())).containsInAnyOrder(this.expectedBadRows);

                this.pipeline.run();
            } catch (ReflectiveOperationException e) {
                fail(e.getMessage());
            }
        }
    }
}
