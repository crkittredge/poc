package org.ascension.addg.gcp.ingestion.read.file;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.ascension.addg.gcp.ingestion.core.Utils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Implementation for reading json files
 * @param <I> Type we are reading from
 */
public final class ReadJSONDoFn<I> extends ReadFileDoFn<I> {

    private static final ObjectReader mapper = new ObjectMapper().reader();

    /**
     * Instantiates a new DoFn
     * @param config step configuration
     * @param recordsTag TupleTag for good messages
     * @param badRecordsTag TupleTag for bad messages
     * @param schemaTag TupleTag for good message schema map
     * @param badSchemaTag TupleTag for bad message schema map
     */
    public ReadJSONDoFn(ReadJSONStep config, TupleTag<Row> recordsTag, TupleTag<Row> badRecordsTag, TupleTag<Map<String, Schema>> schemaTag, TupleTag<Map<String, Schema>> badSchemaTag) {
        super(config, recordsTag, badRecordsTag, schemaTag, badSchemaTag);
    }

    /**
     * Processes a JSON file
     * @param fileName Name of the json file
     * @param bin BufferedInputStream for reading
     * @param c Current process context
     * @throws IOException for IO errors when reading
     */
    @SuppressWarnings("unchecked")
    @Override
    protected void processFile(String fileName, BufferedInputStream bin, ProcessContext c) throws IOException {
        LOG.info("Processing JSON file: {}", fileName);
        var cfg = (ReadFileStep) this.getConfig();

        // if we have configured dynamic destinations, then set the table name in metadata
        if (cfg.getDynamicDestinationConfig() != null) {
            this.setOutputTable(cfg.getDynamicDestinationConfig().getTableName(fileName));
        }

        // add the file name field to the metadata
        this.addMetadata(ReadFileStep.FILE_NAME_FIELD, fileName);

        // build and output the rows
        try (var br = new BufferedReader(new InputStreamReader(bin, StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                try {
                    var row = Utils.jsonToRow(ReadJSONDoFn.mapper.readTree(line), this.getMetadataOptions(), cfg.getReplaceHeaderSpecialCharactersWith());
                    c.output(this.getSchemaTag(), Map.of(this.getOutputTable(), row.getSchema()));
                    c.output(this.getRecordsTag(), row);
                } catch (JsonProcessingException e) {
                    LOG.error("Error parsing JSON record: {}", e.getMessage());

                    if (cfg.getDynamicDestinationConfig() != null) {
                        this.setOutputTable(cfg.getDynamicDestinationConfig().getTableName(fileName) + "_error");
                    }

                    var errorSchema = Utils.getErrorSchema(this.getMetadataOptions());
                    var errorRow = Row.withSchema(errorSchema).addValues(e.getMessage(), line).build();

                    // if land table is defined in read step, add row/schema mapping
                    // and add table name to row metadata
                    c.output(this.getBadSchemaTag(), Map.of(this.getOutputTable(), errorSchema));
                    c.output(this.getBadRecordsTag(), errorRow);
                }
            }
        }
    }
}



