package org.ascension.addg.gcp.ingestion.read.jdbc;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.ascension.addg.gcp.ingestion.core.Utils;
import org.ascension.addg.gcp.ingestion.read.ReaderDoFn;

import java.util.Map;
import java.util.Objects;

/**
 * Implementation fort reading JDBC records
 */
public class ReadJdbcDoFn extends ReaderDoFn<Row> {

    public ReadJdbcDoFn(ReadJdbcStep config, TupleTag<Row> recordsTag, TupleTag<Row> badRecordsTag, TupleTag<Map<String, Schema>> schemaTag, TupleTag<Map<String, Schema>> badSchemaTag) {
        super(config, recordsTag, badRecordsTag, schemaTag, badSchemaTag);
    }

    @ProcessElement
    public final void processElement(ProcessContext c) {
        var row = Objects.requireNonNull(c.element());
        var existingSchema = row.getSchema();
        var cfg = (ReadJdbcStep) this.getConfig();

        var options = Schema.Options.builder().build();

        // start building the new schema
        var schemaBuilder = Schema.builder().setOptions(options);

        // process existing fields to maintain field order
        for (var f: existingSchema.getFields()) {
            schemaBuilder = schemaBuilder.addField(Utils.checkHeaderName(f.getName(), cfg.getReplaceHeaderSpecialCharactersWith()), f.getType());
        }

        // finish creating the schema
        var schema = schemaBuilder.build();

        // start creating the row
        var builder = Row.withSchema(schema);


        // add the values to a new record
        for (var f: existingSchema.getFields()) {
            builder = builder.addValue(row.getValue(f.getName()));
        }

        c.output(this.getSchemaTag(), Map.of("default", schema));
        c.output(this.getRecordsTag(), builder.build());
    }
}
