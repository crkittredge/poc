package org.ascension.addg.gcp.ingestion.transform;

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
public abstract class TransformerDoFn<I> extends BaseDoFn<I> {
    protected String outTable;

    protected TransformerDoFn(BaseStep config, TupleTag<Row> recordsTag, TupleTag<Map<String, Schema>> schemaTag) {
        super(config, recordsTag, schemaTag);
        this.outTable = "default";
    }

    /**
     * Returns the output table stored in the row metadata
     * @return String table name
     */
    protected String getOutputTable() {
        return this.outTable;
    }

    /**
     * Sets the output table stored in the row metadata
     * @param outTable output table name
     */
    protected void setOutputTable(String outTable) {
        this.outTable = outTable;
    }
}
