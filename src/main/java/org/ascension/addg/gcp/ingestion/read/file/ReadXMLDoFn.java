package org.ascension.addg.gcp.ingestion.read.file;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.ascension.addg.gcp.ingestion.core.Utils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Implementation for reading XML files
 * @param <I> Type we are reading from
 */
public final class ReadXMLDoFn<I> extends ReadFileDoFn<I> {

    private static final ObjectReader mapper = new XmlMapper().reader();

    /**
     * Instantiates a new DoFn
     * @param config step configuration
     * @param recordsTag TupleTag for good messages
     * @param badRecordsTag TupleTag for bad messages
     * @param schemaTag TupleTag for good message schema map
     * @param badSchemaTag TupleTag for bad message schema map
     */
    public ReadXMLDoFn(ReadXMLStep config, TupleTag<Row> recordsTag, TupleTag<Row> badRecordsTag, TupleTag<Map<String, Schema>> schemaTag, TupleTag<Map<String, Schema>> badSchemaTag) {
        super(config, recordsTag, badRecordsTag, schemaTag, badSchemaTag);
    }

    @Override
    protected void processFile(String fileName, BufferedInputStream bin, ProcessContext c) throws IOException {
        LOG.info("Processing XML file: {}", fileName);
        var cfg = (ReadFileStep) this.getConfig();

        if (cfg.getDynamicDestinationConfig() != null) {
            this.setOutputTable(cfg.getDynamicDestinationConfig().getTableName(fileName));
        }

        this.addMetadata(ReadFileStep.FILE_NAME_FIELD, fileName);

        // build and output the rows
        var line = bin.readAllBytes();

        try {
            var jsonObj = ReadXMLDoFn.mapper.readTree(line);
            var root = jsonObj.fields();

            while (root.hasNext()) {
                var fields = (ArrayNode) root.next().getValue();

                for (var i = 0; i < fields.size(); i++) {
                    var row = Utils.jsonToRow(fields.get(i), this.getMetadataOptions(), cfg.getReplaceHeaderSpecialCharactersWith());

                    // if land table is defined in read step, add row/schema mapping
                    // and add table name to row metadata
                    c.output(this.getSchemaTag(), Map.of(this.getOutputTable(), row.getSchema()));
                    c.output(this.getRecordsTag(), row);
                }
            }

        } catch (JsonProcessingException e) {
            LOG.error("Error parsing XML record: {}", e.getMessage());

            if (cfg.getDynamicDestinationConfig() != null) {
                this.setOutputTable(cfg.getDynamicDestinationConfig().getTableName(fileName) + "_error");
            }

            var errorSchema = Utils.getErrorSchema(this.getMetadataOptions());
            var errorRow = Row.withSchema(errorSchema).addValues(e.getMessage(), new String(line, StandardCharsets.UTF_8)).build();

            // if land table is defined in read step, add row/schema mapping
            // and add table name to row metadata
            c.output(this.getBadSchemaTag(), Map.of(this.getOutputTable(), errorSchema));
            c.output(this.getBadRecordsTag(), errorRow);
        }
    }
}



