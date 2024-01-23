package org.ascension.addg.gcp.ingestion.transform.flatten;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.ascension.addg.gcp.ingestion.read.ReadStep;
import org.ascension.addg.gcp.ingestion.transform.TransformerDoFn;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Adds Athena-specific metadata
 */
public class FlattenDoFn extends TransformerDoFn<Row> {

    /**
     * Instantiates a new DoFn object
     * @param config Step configuration
     * @param recordsTag TupleTag for records
     * @param schemaTag TupleTag for schema map
     */
    public FlattenDoFn(FlattenStep config, TupleTag<Row> recordsTag, TupleTag<Map<String, Schema>> schemaTag) {
        super(config, recordsTag, schemaTag);
    }

    /**
     * Recursively flattens a nested beam schema
     * @param field beam field to flatten
     * @param object value contained in the field
     * @param sb schema builder object
     * @return schema builder object
     */
    @SuppressWarnings("unchecked")
    private Schema.Builder flattenSchema(Schema.Field field, Object object, Schema.Builder sb) {
        if (object instanceof Map) {
            var type = Objects.requireNonNull(field.getType().getMapValueType());
            var i = 0;
            var mapObj = (Map<String, Object>) object;
            for (var e : mapObj.entrySet()) {
                sb = this.flattenSchema(Schema.Field.of(field.getName() + "_" + e.getKey(), type), e.getValue(), sb);
            }
        } else if (object instanceof List) {
            var type = Objects.requireNonNull(field.getType().getCollectionElementType());
            var listObj = (List<Object>) object;
            for (var i = 0; i < listObj.size(); i++) {
                sb = this.flattenSchema(Schema.Field.of(field.getName() + "_" + (i + 1), type), listObj.get(i), sb);
            }
        } else {
            sb = sb.addField(field);
        }
        return sb;
    }

    /**
     * Adds flat data to a row
     * @param object value to flatten
     * @param rb row builder object
     * @return row builder object
     */
    @SuppressWarnings("unchecked")
    private Row.Builder addFlatData(Object object, Row.Builder rb) {
        if (object instanceof Map) {
            var i = 0;
            var mapObj = (Map<String, Object>) object;
            for (var e : mapObj.entrySet()) {
                rb = this.addFlatData(e.getValue(), rb);
            }
        } else if (object instanceof List) {
            var listObj = (List<Object>) object;
            for (Object o : listObj) {
                rb = this.addFlatData(o, rb);
            }
        } else {
            rb = rb.addValue(object);
        }
        return rb;
    }

    /**
     * Flattens a row with nested values
     * @param row input row
     * @return flattened row
     */
    @SuppressWarnings("unchecked")
    public Row flattenRow(Row inputRow) {

        var originalSchema = inputRow.getSchema();
        var existingOptions = originalSchema.getOptions();
        var sb = new Schema.Builder().setOptions(existingOptions);

        for (var field: originalSchema.getFields()) {
            var value = inputRow.getValue(field.getName());
            sb = this.flattenSchema(field, value, sb);
        }

        var schema = sb.build();
        var rb = Row.withSchema(schema);
        for (var field: originalSchema.getFields()) {
            var value = inputRow.getValue(field.getName());
            rb = this.addFlatData(value, rb);
        }

        return rb.build();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        var existingRow = Objects.requireNonNull(c.element());
        var existingSchema = existingRow.getSchema();
        var existingOptions = existingSchema.getOptions();

        var row = this.flattenRow(existingRow);

        // if the output table is coming from the reader (dynamic destination)
        if (existingOptions.hasOption(ReadStep.OUTPUT_TABLE_FIELD)) {
            this.setOutputTable(Objects.requireNonNull(existingOptions.getValue(ReadStep.OUTPUT_TABLE_FIELD)).toString());
        }

        c.output(this.getSchemaTag(), Map.of(this.getOutputTable(), row.getSchema()));
        c.output(this.getRecordsTag(), row);
    }
}