package org.ascension.addg.gcp.ingestion.read;

import com.typesafe.config.Optional;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.ascension.addg.gcp.ingestion.core.BaseStep;

import java.util.Map;

/**
 * Abstraction for a Reader configuration
 */
public abstract class ReadStep extends BaseStep {
    public static final String OUTPUT_TABLE_FIELD = "internal_OutputTable";
    @Optional private String replaceHeaderSpecialCharactersWith;
    private final Class<? extends Reader<PBegin>> readerClass;

    /**
     * Instantiates a new configuration object
     * @param implementation implementation class name
     * @param readerClass reader class
     */
    protected ReadStep(String implementation, Class<? extends Reader<PBegin>> readerClass) {
        super(implementation);
        this.readerClass = readerClass;
        this.replaceHeaderSpecialCharactersWith = "";
    }

    public final String getReplaceHeaderSpecialCharactersWith() {
        return this.replaceHeaderSpecialCharactersWith;
    }

    public final void setReplaceHeaderSpecialCharactersWith(String replaceHeaderSpecialCharactersWith) {
        this.replaceHeaderSpecialCharactersWith = replaceHeaderSpecialCharactersWith;
    }

    public final Class<? extends Reader<PBegin>> getReaderClass() {
        return this.readerClass;
    }

    @SuppressWarnings("unchecked")
    public <I extends ReaderDoFn<?>> I getImplementationObj(TupleTag<Row> recordsTag, TupleTag<Row> badRecordsTag, TupleTag<Map<String, Schema>> schemaTag, TupleTag<Map<String, Schema>> badSchemaTag) {
        I retVal;
        try {
            var cls = (Class<? extends ReaderDoFn<?>>) Class.forName(this.getImplementation());
            retVal = ((Class<I>)cls).getConstructor(this.getClass(), TupleTag.class, TupleTag.class, TupleTag.class, TupleTag.class)
                    .newInstance(this, recordsTag, badRecordsTag, schemaTag, badSchemaTag);
        } catch (ReflectiveOperationException e) {
            throw new InvalidImplementationException(e);
        }

        return retVal;
    }
}
