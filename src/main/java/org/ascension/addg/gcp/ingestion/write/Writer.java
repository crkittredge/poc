package org.ascension.addg.gcp.ingestion.write;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.*;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;
import org.ascension.addg.gcp.ingestion.core.BaseTransform;
import org.ascension.addg.gcp.ingestion.core.Utils;
import org.ascension.addg.gcp.ingestion.read.ReadStep;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.util.Map;
import java.util.Objects;

/**
 * Abstraction for writing to a target
 */
public abstract class Writer extends BaseTransform<@NonNull PCollectionTuple, @NonNull PDone> {

    /**
     * Instantiates a new writer
     * @param config step configuration
     * @param recordsTag TupleTag for good messages
     * @param badRecordsTag TupleTag for bad messages
     * @param schemaTag TupleTag for good message schema map
     * @param badSchemaTag TupleTag for bad message schema map
     */
    protected Writer(WriteStep config, TupleTag<Row> recordsTag, TupleTag<Row> badRecordsTag, TupleTag<Map<String, Schema>> schemaTag, TupleTag<Map<String, Schema>> badSchemaTag) {
        super(config, recordsTag, badRecordsTag, schemaTag, badSchemaTag);
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized PDone expand(@NonNull PCollectionTuple input) {
        var goodRecords = input.get(this.getRecordsTag());
        var badRecords = input.get(this.getBadRecordsTag());

        var goodSchemaSideInput = input.get(this.getSchemaTag())
                .apply(Combine.globally(Utils.combineHeaders))
                .apply(View.asSingleton());

        var badSchemaSideInput = input.get(this.getBadSchemaTag())
                .apply(Combine.globally(Utils.combineHeaders))
                .apply(View.asSingleton());

        this.writeRows(goodRecords, goodSchemaSideInput, false);
        this.writeRows(badRecords, badSchemaSideInput, true);

        return PDone.in(input.getPipeline());
    }

    /**
     * Factor method to instantiate a new writer object
     * @param step step configuration
     * @param options pipeline options
     * @param recordsTag TupleTag for good messages
     * @param badRecordsTag TupleTag for bad messages
     * @param schemaTag TupleTag for good message schema map
     * @param badSchemaTag TupleTag for bad message schema map
     * @return new Writer object
     * @throws ReflectiveOperationException for errors related to instantiating an object using reflection
     */
    public static Writer of(WriteStep step, IngestionOptions options, TupleTag<Row> recordsTag, TupleTag<Row> badRecordsTag, TupleTag<Map<String, Schema>> schemaTag, TupleTag<Map<String, Schema>> badSchemaTag) throws ReflectiveOperationException {
        return step.getWriterClass().getConstructor(WriteStep.class, IngestionOptions.class, TupleTag.class, TupleTag.class, TupleTag.class, TupleTag.class).newInstance(step, options, recordsTag, badRecordsTag, schemaTag, badSchemaTag);
    }

    /**
     * Writes a PCollection of Row to BigQuery
     * @param input PCollection of Row
     * @param schemaSideInput table->schema side input
     * @param isErrorDestination true if writing to error destination
     */
    protected abstract void writeRows(PCollection<Row> input, PCollectionView<Map<String, Schema>> schemaSideInput, boolean isErrorDestination);

    public static String getDestination(Row row, String defaultTableId) {
        return Objects.requireNonNull(row).getSchema().getOptions().getValueOrDefault(ReadStep.OUTPUT_TABLE_FIELD, defaultTableId);
    }
}
