package org.ascension.addg.gcp.ingestion.read.bigquery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQuery;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.ascension.addg.gcp.BaseTest;
import org.ascension.addg.gcp.ingestion.IngestionTest;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Tests row-level scenarios for reading data from MongoDb
 */
public class ReadBigQueryRowConverterTest extends IngestionTest.Setup {

    /**
     * Set up the test configuration
     */
    public ReadBigQueryRowConverterTest() {
        super(BaseTest.PIPELINE_ARGS, IngestionOptions.class, ReadBigQueryDoFn.class);
    }

    public static class SARCoder extends CustomCoder<SchemaAndRecord> {

        public static SARCoder of() {
            return new SARCoder();
        }

        @Override
        public void encode(SchemaAndRecord value, @UnknownKeyFor @NonNull @Initialized OutputStream outStream) throws @UnknownKeyFor@NonNull@Initialized CoderException, @UnknownKeyFor@NonNull@Initialized IOException {
            var record = Objects.requireNonNull(value).getRecord();
            var tabSchema = value.getTableSchema();
            var schema = BigQueryUtils.fromTableSchema(tabSchema);
            var row = BigQueryUtils.toBeamRow(record, schema, BigQueryUtils.ConversionOptions.builder().build());

            SerializableCoder.of(Row.class).encode(row, outStream);
            SerializableCoder.of(Schema.class).encode(schema, outStream);
        }

        @Override
        public SchemaAndRecord decode(@UnknownKeyFor @NonNull @Initialized InputStream inStream) throws @UnknownKeyFor@NonNull@Initialized CoderException, @UnknownKeyFor@NonNull@Initialized IOException {
            var row = SerializableCoder.of(Row.class).decode(inStream);
            var schema = SerializableCoder.of(Schema.class).decode(inStream);
            return new SchemaAndRecord(AvroUtils.toGenericRecord(row), BigQueryUtils.toTableSchema(schema));
        }
    }

    protected PCollection<SchemaAndRecord> getSARPCollection() {
        var sars = new ArrayList<SchemaAndRecord>();

        for (var i = 0; i < 10; i++) {
            var rec = new GenericRecordBuilder(SchemaBuilder.builder().record("rec").namespace("rec").fields().nullableString("field_1", null).nullableString("field_2", null).endRecord()).set("field_1", String.valueOf(i)).set("field_2", "TEST").build();
            var sch = new TableSchema().setFields(List.of(new TableFieldSchema().setName("field_1").setType("STRING").setMode("NULLABLE"), new TableFieldSchema().setName("field_2").setType("STRING").setMode("NULLABLE")));
            sars.add(new SchemaAndRecord(rec, sch));
        }

        return this.pipeline.apply(Create.of(sars).withCoder(SARCoder.of()));
    }

    @Override
    public void executeTest() {
        var expectedRows = new ArrayList<Row>();
        var expectedSchema = Schema.builder().addNullableStringField("field_1").addNullableStringField("field_2").build();

        for (var i = 0; i < 10; i++) {
            expectedRows.add(Row.withSchema(expectedSchema).addValues(String.valueOf(i), "TEST").build());
        }

        var inPC = this.getSARPCollection();
        var outPC = inPC.apply(MapElements.into(TypeDescriptor.of(Row.class)).via(new BigQueryReader.SchemaAndRecordToRow())).setCoder(SerializableCoder.of(Row.class));

        PAssert.that(outPC).containsInAnyOrder(expectedRows);
        this.pipeline.run();
    }
}
