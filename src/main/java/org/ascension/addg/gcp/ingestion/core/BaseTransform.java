package org.ascension.addg.gcp.ingestion.core;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.*;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;

/**
 * Base abstraction for PTransforms
 * @param <I> Input collection
 * @param <O> Output collection
 */
public abstract class BaseTransform<I extends PInput, O extends POutput> extends PTransform<@NonNull I, @NonNull O> {

    /**
     * reusable logger
     */
    protected static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final BaseStep config;
    private final TupleTag<Row> recordsTag;
    private final TupleTag<Row> badRecordsTag;
    private final TupleTag<Map<String, Schema>> schemaTag;
    private final TupleTag<Map<String, Schema>> badSchemaTag;

    /**
     * Instantiates a new object
     * @param config step configuration
     * @param recordsTag TupleTag for good records
     * @param badRecordsTag TupleTag for bad records
     * @param schemaTag TupleTag for good record schema map
     * @param badSchemaTag TupleTag for bad record schema map
     */
    protected BaseTransform(BaseStep config, TupleTag<Row> recordsTag, TupleTag<Row> badRecordsTag, TupleTag<Map<String, Schema>> schemaTag, TupleTag<Map<String, Schema>> badSchemaTag) {
        this.config = config;
        this.recordsTag = recordsTag;
        this.badRecordsTag = badRecordsTag;
        this.schemaTag = schemaTag;
        this.badSchemaTag = badSchemaTag;
    }

    /**
     * Returns the TupleTag for good records
     * @return TupleTag
     */
    public final TupleTag<Row> getRecordsTag() {
        return this.recordsTag;
    }

    /**
     * Returns the TupleTag for bad records
     * @return TupleTag
     */
    public final TupleTag<Row> getBadRecordsTag() {
        return this.badRecordsTag;
    }

    /**
     * Returns the TupleTag for good record schema map
     * @return TupleTag
     */
    public final TupleTag<Map<String, Schema>> getSchemaTag() {
        return this.schemaTag;
    }

    /**
     * Returns the TupleTag for bad record schema map
     * @return TupleTag
     */
    public final TupleTag<Map<String, Schema>> getBadSchemaTag() {
        return this.badSchemaTag;
    }

    /**
     * Returns the Step configuration associated with the caller
     * @return BaseStep
     */
    protected final BaseStep getConfig() {
        return this.config;
    }
}
