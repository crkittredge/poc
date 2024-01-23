package org.ascension.addg.gcp.ingestion.read;

import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.TupleTag;
import org.ascension.addg.gcp.ingestion.core.BaseTransform;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * An abstraction for reading data from a source
 * @param <I> Type being read from
 */
public abstract class Reader<I extends PInput> extends BaseTransform<@NonNull I, @NonNull PCollectionTuple> {
    protected final IngestionOptions options;

    /**
     * Initializes an instance of this class
     * @param config step config
     * @param options pipeline options
     */
    protected Reader(ReadStep config, IngestionOptions options) {
        super(config, new TupleTag<>(), new TupleTag<>(), new TupleTag<>(), new TupleTag<>());
        this.options = options;
    }

    public static Reader<PBegin> of(ReadStep step, IngestionOptions options) throws ReflectiveOperationException {
        return step.getReaderClass().getConstructor(ReadStep.class, IngestionOptions.class).newInstance(step, options);
    }
}
