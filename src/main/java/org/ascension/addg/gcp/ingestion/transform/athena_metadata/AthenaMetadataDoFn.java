package org.ascension.addg.gcp.ingestion.transform.athena_metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.io.FilenameUtils;
import org.ascension.addg.gcp.ingestion.core.Utils;
import org.ascension.addg.gcp.ingestion.read.ReadStep;
import org.ascension.addg.gcp.ingestion.read.file.ReadFileStep;
import org.ascension.addg.gcp.ingestion.transform.TransformerDoFn;

import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Adds Athena-specific metadata
 */
public class AthenaMetadataDoFn extends TransformerDoFn<Row> {

    private static final Pattern MINISTRY_ID_REGEX = Pattern.compile("\\d+$");
    private static final Pattern FILE_DATE_REGEX = Pattern.compile("_\\d+_");
    private final Map<String, String> ministryJson;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Parses and returns a ministry id from a given String (e.g. filename).
     * @param fileName The string to parse
     * @return The ministry id if found, otherwise null
     */
    public static String getMinistryID(String fileName) {
        var m = AthenaMetadataDoFn.MINISTRY_ID_REGEX.matcher(FilenameUtils.removeExtension(fileName));
        return m.find() ? m.group(0) : null;
    }

    /**
     * Parses and returns a file date from a given String (e.g. filename).
     * @param fileName The string to parse
     * @return The file date if found, otherwise null
     */
    public static String getFileDate(String fileName) {
        var m = AthenaMetadataDoFn.FILE_DATE_REGEX.matcher(FilenameUtils.removeExtension(fileName));
        return m.find() ? m.group(0).replace("_", "") : null;
    }

    /**
     * Custom exception for processing Athena metadata
     */
    public static final class AthenaMetadataRuntimeException extends RuntimeException {

        /**
         * Instantiates a new Exception with a string error message
         * @param e Error message
         */
        public AthenaMetadataRuntimeException(String e) {
            super(e);
        }
    }

    /**
     * Instantiates a new DoFn object
     * @param config Step configuration
     * @param recordsTag TupleTag for records
     * @param schemaTag TupleTag for schema map
     */
    public AthenaMetadataDoFn(AthenaMetadataStep config, TupleTag<Row> recordsTag, TupleTag<Map<String, Schema>> schemaTag) {
        super(config, recordsTag, schemaTag);

        try {
            this.ministryJson = AthenaMetadataDoFn.MAPPER.readValue(Utils.readFromGCS(
                    ValueProvider.StaticValueProvider.of(config.getMinistryLookupPath())).get(), new TypeReference<>() {});
        } catch (JsonProcessingException e){
            throw new AthenaMetadataDoFn.AthenaMetadataRuntimeException("Error parsing json file: " + e.getMessage());
        }
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        var existingRow = Objects.requireNonNull(c.element());
        var existingSchema = existingRow.getSchema();
        var existingOptions = existingSchema.getOptions();
        var fileName = Objects.requireNonNull(existingOptions.getValue(ReadFileStep.FILE_NAME_FIELD)).toString();
        var ministryIdValue = getMinistryID(fileName);

        var schemaOptions = Schema.Options.builder()
                .addOptions(existingOptions)
                .setOption("meta_ministryid", Schema.FieldType.STRING.withNullable(true), ministryIdValue)
                .setOption("meta_ministry", Schema.FieldType.STRING.withNullable(true), this.ministryJson.get(ministryIdValue))
                .setOption("meta_filedate", Schema.FieldType.STRING.withNullable(true), getFileDate(fileName)).build();

        var schema = existingSchema.withOptions(schemaOptions);
        var row = Row.withSchema(schema).addValues(existingRow.getValues()).build();

        // if the output table is coming from the reader (dynamic destination)
        if (schemaOptions.hasOption(ReadStep.OUTPUT_TABLE_FIELD)) {
            this.setOutputTable(Objects.requireNonNull(schemaOptions.getValue(ReadStep.OUTPUT_TABLE_FIELD)).toString());
        }

        c.output(this.getSchemaTag(), Map.of(this.getOutputTable(), row.getSchema()));
        c.output(this.getRecordsTag(), row);
    }
}