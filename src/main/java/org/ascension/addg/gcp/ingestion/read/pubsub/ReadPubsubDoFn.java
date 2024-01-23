package org.ascension.addg.gcp.ingestion.read.pubsub;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.ascension.addg.gcp.ingestion.core.Utils;
import org.ascension.addg.gcp.ingestion.read.ReadStep;
import org.ascension.addg.gcp.ingestion.read.ReaderDoFn;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

/**
 * Implementation for reading PubsubMessages
 */
public class ReadPubsubDoFn extends ReaderDoFn<PubsubMessage> {

    private static final ObjectReader mapper = new ObjectMapper().reader();

    public ReadPubsubDoFn(ReadPubsubStep config, TupleTag<Row> recordsTag, TupleTag<Row> badRecordsTag, TupleTag<Map<String, Schema>> schemaTag, TupleTag<Map<String, Schema>> badSchemaTag) {
        super(config, recordsTag, badRecordsTag, schemaTag, badSchemaTag);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        var element = Objects.requireNonNull(c.element());
        var payload = new String(element.getPayload(), StandardCharsets.UTF_8);
        var messageId = Objects.requireNonNull(element.getMessageId());
        var attributeMap = element.getAttributeMap();
        var cfg = (ReadStep) this.getConfig();

        LOG.info("Processing Pub/Sub message: {}", messageId);

        var timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
        timestampFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        // add metadata to schema options
        var attributes = new ArrayList<String>();
        if (attributeMap != null) {
            attributeMap.forEach((key, value) -> attributes.add(String.format("%s=%s", key, value)));
        }

        var options = Schema.Options.builder()
                .setOption("meta_pubsub_message_id", Schema.FieldType.STRING, messageId)
                .setOption("meta_pubsub_attributes", Schema.FieldType.STRING, String.join(",", attributes))
                .build();

        try {
            var row = Utils.jsonToRow(ReadPubsubDoFn.mapper.readTree(payload), options, cfg.getReplaceHeaderSpecialCharactersWith());

            c.output(this.getSchemaTag(), Map.of("default", row.getSchema()));
            c.output(this.getRecordsTag(), row);

        } catch (JsonProcessingException e){
            LOG.error("Error parsing JSON record: {}", e.getMessage());

            var errorOptions = Schema.Options.builder().addOptions(options);

            var errorSchema = Utils.getErrorSchema(errorOptions.build());

            var errorRow = Row.withSchema(errorSchema).addValues(e.getMessage(), payload).build();

            c.output(this.getBadSchemaTag(), Map.of("default", errorSchema));
            c.output(this.getBadRecordsTag(), errorRow);
        }
    }
}
