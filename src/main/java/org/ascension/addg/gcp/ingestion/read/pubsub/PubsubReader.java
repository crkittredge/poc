package org.ascension.addg.gcp.ingestion.read.pubsub;

import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.lang3.ObjectUtils;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;
import org.ascension.addg.gcp.ingestion.read.Reader;
import org.ascension.addg.gcp.ingestion.read.ReadStep;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.util.List;

/**
 * Implements a Pub/Sub Reader
 */
public final class PubsubReader extends Reader<PBegin> {

    private final String topic;
    private final String subscription;

    /**
     * Custom exception when the provided source is not valid
     */
    public static class NoValidSourceException extends RuntimeException {

        /**
         * Instantiates a new exception
         * @param m String error message
         */
        public NoValidSourceException(String m) {
            super(m);
        }
    }

    /**
     * Instantiates a new PubsubReader object
     * @param config step configuration
     * @param options pipeline options
     */
    public PubsubReader(ReadStep config, IngestionOptions options) {
        super(config, options);
        var cfg = (ReadPubsubStep) config;

        this.topic = cfg.getTopic();
        this.subscription = cfg.getSubscription();

        // make sure one of either topic or subscription were provided
        if (!ObjectUtils.anyNotNull(this.topic, this.subscription)) {
            throw new PubsubReader.NoValidSourceException("One of topic or subscription are required");
        }
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized PCollectionTuple expand(PBegin input) {
        var cfg = (ReadPubsubStep) this.getConfig();
        PCollectionTuple result;

        var messages = input.apply("Read from Pub/Sub", this.topic != null ?
                PubsubIO.readMessagesWithAttributesAndMessageId().fromTopic(this.topic) :
                PubsubIO.readMessagesWithAttributesAndMessageId().fromSubscription(this.subscription));

        result = messages.apply("Parse Messages",
                ParDo.of(cfg.<ReadPubsubDoFn>getImplementationObj(this.getRecordsTag(), this.getBadRecordsTag(), this.getSchemaTag(), this.getBadSchemaTag()))
                        .withOutputTags(this.getRecordsTag(), TupleTagList.of(List.of(this.getBadRecordsTag(), this.getSchemaTag(), this.getBadSchemaTag()))));

        result = PCollectionTuple
                .of(this.getRecordsTag(), result.get(this.getRecordsTag()).setCoder(SerializableCoder.of(Row.class)))
                .and(this.getBadRecordsTag(), result.get(this.getBadRecordsTag()).setCoder(SerializableCoder.of(Row.class)))
                .and(this.getSchemaTag(), result.get(this.getSchemaTag()).setCoder(MapCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Schema.class))))
                .and(this.getBadSchemaTag(), result.get(this.getBadSchemaTag()).setCoder(MapCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Schema.class))));

        return result;
    }
}
