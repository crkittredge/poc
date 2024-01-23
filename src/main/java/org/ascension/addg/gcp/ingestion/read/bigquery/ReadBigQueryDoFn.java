package org.ascension.addg.gcp.ingestion.read.bigquery;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.ascension.addg.gcp.ingestion.read.ReaderDoFn;

import java.util.Map;
import java.util.Objects;

/**
 * Implementation for reading Bigquery records
 */
public class ReadBigQueryDoFn extends ReaderDoFn<Row> {

    public ReadBigQueryDoFn(ReadBigQueryStep config, TupleTag<Row> recordsTag, TupleTag<Row> badRecordsTag, TupleTag<Map<String, Schema>> schemaTag, TupleTag<Map<String, Schema>> badSchemaTag) {
        super(config, recordsTag, badRecordsTag, schemaTag, badSchemaTag);
    }

    @ProcessElement
    public final void processElement(ProcessContext c) {
        var row = Objects.requireNonNull(c.element());
        c.output(this.getSchemaTag(), Map.of("default", row.getSchema()));
        c.output(this.getRecordsTag(), row);
    }
}
