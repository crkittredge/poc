package org.ascension.addg.gcp;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;
import org.ascension.addg.gcp.ingestion.core.Utils;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Rule;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

public abstract class BaseTest {

    @Rule public final TestPipeline pipeline;
    @Rule public MockitoRule rule = MockitoJUnit.rule();

    protected static final String CONFIG_PATH = "gs://some_bucket/some_file.conf";
    protected static final String TEST_PROJECT = "some_project";
    protected static final String TEST_DATASET = "some_dataset";
    protected static final String TEST_TABLE = "some_table";
    private final ListAppender<ILoggingEvent> logAppender;
    protected IngestionOptions pipelineOptions;
    protected MockedStatic<Utils> utilsMock;

    protected BaseTest(String[] pipelineArgs, Class<? extends IngestionOptions> pipelineOptionsClazz, Class<?> loggerClazz) {
        this.logAppender = BaseTest.getLogAppender(loggerClazz);
        this.pipelineOptions = BaseTest.getPipelineOptions(pipelineArgs, pipelineOptionsClazz);
        this.pipeline = TestPipeline.fromOptions(this.pipelineOptions);

        this.utilsMock = mockStatic(Utils.class);

        this.utilsMock.when(() -> Utils.getErrorSchema(any())).thenCallRealMethod();
    }

    protected static String[] PIPELINE_ARGS = new String[]{
            "--project=" + BaseTest.TEST_PROJECT,
            "--pipelineConfig=" + BaseTest.CONFIG_PATH,
            "--runner=DirectRunner",
            "--stagingLocation=gs://testing1_temp1/", //gs://some_bucket/some_temp_path",
            "--gcpTempLocation=gs://testing1_temp1/", //some_bucket/some_temp_path",
            "--jobNameOverride=UnitTestJob",
            "--waitUntilFinish=true",
            "--stableUniqueNames=OFF",
            "--tempLocation=gs://testing1_temp1/", //some_bucket/some_temp_path"
            "--enforceImmutability=false"
    };

    /**
     * Attaches a ListAppender to a log to capture events
     * @param c Class name
     * @return ListAppender
     */
    private static ListAppender<ILoggingEvent> getLogAppender(Class<?> c) {
        var fooLogger = (Logger) LoggerFactory.getLogger(c);
        var listAppender = new ListAppender<ILoggingEvent>();
        listAppender.start();
        fooLogger.addAppender(listAppender);
        return listAppender;
    }

    /**
     * Performs assertions against log messages
     * @param message message to assert
     */
    protected void assertLog(String message, Level level) {
        var hasLog = false;
        for (var l: this.logAppender.list) {
            var logMessage = l.getFormattedMessage();
            var logLevel = l.getLevel();
            if (logMessage.equals(message) && logLevel.equals(level)) {
                hasLog = true;
                break;
            }
        }

        if (!hasLog) {
            var logs = new ArrayList<String>();
            for (var l: this.logAppender.list) {
                logs.add(String.format("[%s] %s", l.getLevel().levelStr, l.getFormattedMessage()));
            }
            throw new AssertionError("Expected log:\n[" + level.levelStr + "] " + message + "\nActual logs:\n" + String.join("\n", logs));
        }

    }

    @After public void tearDown() {
        this.utilsMock.close();
    }

    protected TupleTag<Row> recordsTag = new TupleTag<>();
    protected TupleTag<Row> badRecordsTag = new TupleTag<>();
    protected TupleTag<Map<String, Schema>> schemaTag = new TupleTag<>();
    protected TupleTag<Map<String, Schema>> badSchemaTag = new TupleTag<>();

    protected PCollectionTuple getTestPCollectionTuple() {
        return PCollectionTuple.of(this.recordsTag, this.getTestRowPCollection())
                .and(this.badRecordsTag, this.getTestBadRowPCollection());
    }

    protected Schema getTestSchema() {
        return Schema.builder()
                .addField(Schema.Field.of("field_1", Schema.FieldType.STRING))
                .addField(Schema.Field.of("field_2", Schema.FieldType.INT32))
                .addField(Schema.Field.of("field_3", Schema.FieldType.DOUBLE)).build();
    }

    protected PCollection<Map<String, Schema>> getTestSchemaPCollection() {
        return this.pipeline.apply(Create.<Map<String, Schema>>of(Map.of("land_table", this.getTestSchema()))
                .withCoder(MapCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Schema.class))));
    }

    protected PCollection<Row> getTestRowPCollection() {
        var rows = new ArrayList<Row>();
        var schema = this.getTestSchema();

        for (var i = 0; i < 10; i++) {
            var row = Row.withSchema(schema).addValues("1", 2, 3.0).build();
            rows.add(row);
        }

        return this.pipeline.apply(Create.of(rows));
    }

    protected PCollection<Row> getTestBadRowPCollection() {
        var rows = new ArrayList<Row>();
        var schema = Schema.builder()
                .addField(Schema.Field.of("errorMessage", Schema.FieldType.STRING))
                .addField(Schema.Field.of("rawData", Schema.FieldType.DATETIME))
                .build();

        for (var i = 0; i < 10; i++) {
            var row = Row.withSchema(schema).addValues("error message "  + i, DateTime.parse("2023-10-22T00:00")).build();
            rows.add(row);
        }

        return this.pipeline.apply(Create.of(rows));
    }

    protected PCollection<TableRow> getTestTableRowPCollection() {
        var tableRows = new ArrayList<TableRow>();
        for (var i = 0; i < 10; i++) {
            var row = new TableRow();
            row.put("field_1", 1);
            row.put("field_2", 2);
            row.put("field_3", 3);
            tableRows.add(row);
        }

        return this.pipeline.apply(Create.of(tableRows).withCoder(TableRowJsonCoder.of()));
    }

    protected static ValueProvider<String> readConf(String fileName) {
        return ValueProvider.StaticValueProvider.of(BaseTest.readConfAsString(fileName));
    }

    protected static IngestionOptions getPipelineOptions(String[] pipelineArgs, Class<? extends IngestionOptions> optionsClass) {
        return PipelineOptionsFactory.fromArgs(pipelineArgs).as(optionsClass);
    }

    protected static String readConfAsString(String fileName) {
        String retVal;
        try {
            retVal = Files.readString(Paths.get(Objects.requireNonNull(BaseTest.class.getClassLoader().getResource(fileName)).toURI()));;
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException(e);
        }

        return retVal;
    }

    public static <T> void printPCollection(PCollection<T> pc) {
        pc.apply(UUID.randomUUID().toString(), ParDo.of(new DoFn<T, T>() {
            @DoFn.ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println(Objects.requireNonNullElse(c.element(), ""));
            }
        }));
    }
}
