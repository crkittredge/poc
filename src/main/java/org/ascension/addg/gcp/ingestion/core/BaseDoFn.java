package org.ascension.addg.gcp.ingestion.core;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;

/**
 * Abstraction for row-level transformations into beam Row
 * @param <I> Type we are transforming into a Row
 */
public abstract class BaseDoFn<I> extends DoFn<I, Row> {

    /**
     * reusable logger
     */
    protected static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final BaseStep config;
    private final TupleTag<Row> recordsTag;
    private final TupleTag<Map<String, Schema>> schemaTag;

    /**
     * Instantiates a new DoFn
     * @param config Step configuration from the caller
     * @param recordsTag TupleTag for records
     * @param schemaTag TupleTag for schema map
     */
    protected BaseDoFn(BaseStep config, TupleTag<Row> recordsTag, TupleTag<Map<String, Schema>> schemaTag) {
        this.config = config;
        this.recordsTag = recordsTag;
        this.schemaTag = schemaTag;
    }

    /**
     * Returns the TupleTag for the good records PCollection
     * @return TupleTag of Row
     */
    public TupleTag<Row> getRecordsTag() {
        return this.recordsTag;
    }

    /**
     * Returns the TupleTag for the good records Schema map
     * @return TupleTag of Map
     */
    public TupleTag<Map<String, Schema>> getSchemaTag() {
        return this.schemaTag;
    }

    /**
     * Returns the current step configuration
     * @return BaseStep
     */
    public BaseStep getConfig() {
        return this.config;
    }
}
