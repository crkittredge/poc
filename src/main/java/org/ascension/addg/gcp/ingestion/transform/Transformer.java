package org.ascension.addg.gcp.ingestion.transform;

import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.ascension.addg.gcp.ingestion.core.BaseTransform;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Map;

/**
 * Abstraction to transform a Row
 */
public class Transformer extends BaseTransform<@NonNull PCollectionTuple, @NonNull PCollectionTuple> {

    /**
     * Factory to return an instance of a Transformer
     * @param step TransformStep config object
     * @param recordsTag good messages tag from reader
     * @param badRecordsTag bad messages tag from reader
     * @return Metadata ptransform
     */
    public static Transformer of(TransformerStep step, TupleTag<Row> recordsTag, TupleTag<Row> badRecordsTag, TupleTag<Map<String, Schema>> schemaTag, TupleTag<Map<String, Schema>> badSchemaTag) {
        return  new Transformer(step, recordsTag, badRecordsTag, schemaTag, badSchemaTag);
    }

    protected Transformer(TransformerStep config, TupleTag<Row> recordsTag, TupleTag<Row> badRecordsTag, TupleTag<Map<String, Schema>> schemaTag, TupleTag<Map<String, Schema>> badSchemaTag) {
        super(config, recordsTag, badRecordsTag, schemaTag, badSchemaTag);
    }

    @SuppressWarnings("unchecked")
    @Override
    public @NonNull PCollectionTuple expand(@NonNull PCollectionTuple input) {
        var goodRecords = input.get(this.getRecordsTag()).apply("Apply transformation to good records",
                ParDo.of((TransformerDoFn<Row>) this.getConfig().getImplementationObj(this.getRecordsTag(), this.getSchemaTag()))
                        .withOutputTags(this.getRecordsTag(), TupleTagList.of(List.of(this.getSchemaTag()))));
        var badRecords = input.get(this.getBadRecordsTag()).apply("Apply transformation to bad records",
                ParDo.of((TransformerDoFn<Row>) this.getConfig().getImplementationObj(this.getBadRecordsTag(), this.getBadSchemaTag()))
                        .withOutputTags(this.getBadRecordsTag(), TupleTagList.of(List.of(this.getBadSchemaTag()))));

        return PCollectionTuple
                .of(this.getRecordsTag(), goodRecords.get(this.getRecordsTag()).setCoder(SerializableCoder.of(Row.class)))
                .and(this.getBadRecordsTag(), badRecords.get(this.getBadRecordsTag()).setCoder(SerializableCoder.of(Row.class)))
                .and(this.getSchemaTag(), goodRecords.get(this.getSchemaTag()).setCoder(MapCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Schema.class))))
                .and(this.getBadSchemaTag(), badRecords.get(this.getBadSchemaTag()).setCoder(MapCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Schema.class))));

    }
}
