package org.ascension.addg.gcp.ingestion.read.mongodb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.ascension.addg.gcp.ingestion.core.Utils;
import org.ascension.addg.gcp.ingestion.read.ReadStep;
import org.ascension.addg.gcp.ingestion.read.ReaderDoFn;
import org.bson.Document;

import java.util.Map;
import java.util.Objects;

/**
 * Implementation for reading MongoDb records
 */
public class ReadMongoDbDoFn extends ReaderDoFn<Document> {

    private static final ObjectReader mapper = new ObjectMapper().reader();

    public ReadMongoDbDoFn(ReadMongoDbStep config, TupleTag<Row> recordsTag, TupleTag<Row> badRecordsTag, TupleTag<Map<String, Schema>> schemaTag, TupleTag<Map<String, Schema>> badSchemaTag) {
        super(config, recordsTag, badRecordsTag, schemaTag, badSchemaTag);
    }

    @ProcessElement
    public final void processElement(ProcessContext c) throws JsonProcessingException {
        var payload = Objects.requireNonNull(c.element());
        var cfg = (ReadStep) this.getConfig();

        var row = Utils.jsonToRow(ReadMongoDbDoFn.mapper.readTree(payload.toJson()), Schema.Options.builder().build(), cfg.getReplaceHeaderSpecialCharactersWith());

        c.output(this.getSchemaTag(), Map.of("default", row.getSchema()));
        c.output(this.getRecordsTag(), row);
    }
}
