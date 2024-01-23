package org.ascension.addg.gcp.ingestion.read.mongodb;

import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;
import org.ascension.addg.gcp.ingestion.read.ReadStep;
import org.ascension.addg.gcp.ingestion.read.Reader;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.util.List;

/**
 * Implementation of a MongoDb Reader
 */
public final class MongoDbReader extends Reader<PBegin> {

    /**
     * Instantiates a new object
     * @param config Step configuration
     * @param options Pipeline options
     */
    public MongoDbReader(ReadStep config, IngestionOptions options) {
        super(config, options);
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized PCollectionTuple expand(@NonNull PBegin input) {
        var cfg = (ReadMongoDbStep) this.getConfig();
        PCollectionTuple result;

        var readObj = MongoDbIO.read()
                .withUri(cfg.getUri())
                .withDatabase(cfg.getDatabase())
                .withCollection(cfg.getCollection());

        result = input.apply("Read from MongoDb", readObj)
                .apply("Parse Messages",
                        ParDo.of(cfg.<ReadMongoDbDoFn>getImplementationObj(this.getRecordsTag(), this.getBadRecordsTag(), this.getSchemaTag(), this.getBadSchemaTag()))
                                .withOutputTags(this.getRecordsTag(), TupleTagList.of(List.of(this.getBadRecordsTag(), this.getSchemaTag(), this.getBadSchemaTag()))));

        result = PCollectionTuple
                .of(this.getRecordsTag(), result.get(this.getRecordsTag()).setCoder(SerializableCoder.of(Row.class)))
                .and(this.getBadRecordsTag(), result.get(this.getBadRecordsTag()).setCoder(SerializableCoder.of(Row.class)))
                .and(this.getSchemaTag(), result.get(this.getSchemaTag()).setCoder(MapCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Schema.class))))
                .and(this.getBadSchemaTag(), result.get(this.getBadSchemaTag()).setCoder(MapCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Schema.class))));

        return result;
    }

}
