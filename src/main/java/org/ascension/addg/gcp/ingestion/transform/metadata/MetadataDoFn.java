package org.ascension.addg.gcp.ingestion.transform.metadata;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.ascension.addg.gcp.ingestion.read.ReadStep;
import org.ascension.addg.gcp.ingestion.transform.TransformerDoFn;
import org.joda.time.Instant;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Generic metadata implementation
 */
public class MetadataDoFn extends TransformerDoFn<Row> {

    /**
     * current timestamp for metadata injection
     */
    public static Instant LOAD_TIMESTAMP = Instant.now();

    /**
     * Instantiates a new DoFn object
     * @param config step configuration
     * @param recordsTag TupleTag of records
     * @param schemaTag TupleTag of schema map
     */
    public MetadataDoFn(MetadataStep config, TupleTag<Row> recordsTag, TupleTag<Map<String, Schema>> schemaTag) {
        super(config, recordsTag, schemaTag);
    }

    /**
     * Processed each record in the collection
     * @param c Current process context
     */
    @ProcessElement
    public void processElement(ProcessContext c) {
        var row = this.injectMetadata(Objects.requireNonNull(c.element()));
        c.output(this.getSchemaTag(), Map.of(this.getOutputTable(), row.getSchema()));
        c.output(this.getRecordsTag(), row);
    }

    /**
     * Add metadata columns to an existing Row
     * @param row input Row
     * @return modified Row
     */
    protected Row injectMetadata(Row row) {
        var cfg = (MetadataStep) this.getConfig();
        var existingSchema = row.getSchema();
        var existingOptions = existingSchema.getOptions();
        var renameMap = new HashMap<String, String>();

        // rename any columns if needed
        var fieldsToRename = cfg.getFieldsToRename();

        // make a map of source/target field names
        fieldsToRename.forEach(f -> {
            var fromField = f.getFromField();
            var toField = f.getToField();

            if (existingSchema.hasField(fromField)) {
                renameMap.put(fromField, toField);
            }
        });

        // start building the new schema
        var schemaOptions = Schema.Options.builder()
                .addOptions(existingOptions)
                .setOption(cfg.getLoadTimestampFieldName(), Schema.FieldType.DATETIME, MetadataDoFn.LOAD_TIMESTAMP).build();

        var sortedSchemaOptions = schemaOptions.getOptionNames().stream().sorted().collect(Collectors.toList());
        var schemaBuilder = new Schema.Builder().setOptions(schemaOptions);

        // add sorted columns for metadata fields at the front of the schema
        for (var o : sortedSchemaOptions) {
            if (!o.startsWith("internal_")) {
                schemaBuilder = schemaBuilder.addField(o, schemaOptions.getType(o));
            }
        }

        // process existing fields to maintain field order
        for (var f: existingSchema.getFields()) {
            schemaBuilder = schemaBuilder.addField(renameMap.getOrDefault(f.getName(), f.getName()), f.getType().withNullable(f.getType().getNullable()));
        }

        // finish creating the schema
        var schema = schemaBuilder.build();

        // start creating the row
        var builder = Row.withSchema(schema);

        // add the metadata values
        for (var o : sortedSchemaOptions) {
            if (!o.startsWith("internal_")) {
                builder = builder.addValue(schemaOptions.getValue(o));
            }
        }

        // add the values to a new record
        for (var f: existingSchema.getFields()) {
            builder = builder.addValue(row.getValue(f.getName()));
        }

        // if the output table is coming from the reader (dynamic destination)
        if (schemaOptions.hasOption(ReadStep.OUTPUT_TABLE_FIELD)) {
            this.setOutputTable(Objects.requireNonNull(schemaOptions.getValue(ReadStep.OUTPUT_TABLE_FIELD)).toString());
        }

        return builder.build();
    }
}
