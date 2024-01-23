package org.ascension.addg.gcp.ingestion.read;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.ascension.addg.gcp.ingestion.core.BaseDoFn;
import org.ascension.addg.gcp.ingestion.core.BaseStep;

import java.util.Map;

/**
 * Row-level abstraction for reading
 * @param <I> Type we are reading from
 */
public abstract class ReaderDoFn<I> extends BaseDoFn<I> {
    private final TupleTag<Row> badRecordsTag;
    private final TupleTag<Map<String, Schema>> badSchemaTag;

    protected ReaderDoFn(BaseStep config, TupleTag<Row> recordsTag, TupleTag<Row> badRecordsTag, TupleTag<Map<String, Schema>> schemaTag, TupleTag<Map<String, Schema>> badSchemaTag) {
        super(config, recordsTag, schemaTag);
        this.badRecordsTag = badRecordsTag;
        this.badSchemaTag = badSchemaTag;
    }

    public TupleTag<Row> getBadRecordsTag() {
        return this.badRecordsTag;
    }

    public TupleTag<Map<String, Schema>> getBadSchemaTag() {
        return this.badSchemaTag;
    }
}
