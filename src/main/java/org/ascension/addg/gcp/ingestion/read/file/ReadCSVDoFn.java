package org.ascension.addg.gcp.ingestion.read.file;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.csv.CSVParser;
import org.ascension.addg.gcp.ingestion.core.Utils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Implementation for reading delimited files
 * @param <I> Type we are reading from
 */
public final class ReadCSVDoFn<I> extends ReadFileDoFn<I> {

    /**
     * Instantiates a new DoFn
     * @param config step configuration
     * @param recordsTag TupleTag for good messages
     * @param badRecordsTag TupleTag for bad messages
     * @param schemaTag TupleTag for good message schema map
     * @param badSchemaTag TupleTag for bad message schema map
     */
    public ReadCSVDoFn(ReadCSVStep config, TupleTag<Row> recordsTag, TupleTag<Row> badRecordsTag, TupleTag<Map<String, Schema>> schemaTag, TupleTag<Map<String, Schema>> badSchemaTag) {
        super(config, recordsTag, badRecordsTag, schemaTag, badSchemaTag);
    }

    /**
     * Returns a list of headers for the current file
     * @param cfg Current step configuration
     * @param parsedHeader Header parsed from CSV Parser
     * @param columnCount Number of columns from CSV parser
     * @param fileName Name of the current file
     * @return List of String headers
     */
    private List<String> getHeaders(ReadCSVStep cfg, List<String> parsedHeader, int columnCount, String fileName) {

        List<String> header = null;

        // if mapping of patterns/headers is provided via fileInfo.headerInfo
        if (!cfg.getHeader().isEmpty()) {
            LOG.info("Checking headerInfo for matching file pattern");

            // look for a matching regex
            for (var e : cfg.getHeader()) {
                var filePattern = e.getFilePattern();
                if (Pattern.compile(filePattern, Pattern.CASE_INSENSITIVE).matcher(fileName).matches()) {
                    header = e.getColumns();
                    LOG.info("Found matching filePattern in headerInfo: {}", filePattern);
                    break;
                }
            }

            // pattern wasn't found, use a generic header
            if (header == null) {
                LOG.warn("No matching pattern found in headerInfo");
                header = generateGenericHeader(columnCount);
            }

            // otherwise parse the header from the file if hasHeader is true
        } else if (cfg.getHasHeader()) {

            LOG.info("Using header parsed from file");
            header = parsedHeader;

        // hasHeader = false, create a generic header
        } else {
            LOG.info("File does not have a header");
            header = generateGenericHeader(columnCount);
        }

        return header.stream().map(x -> Utils.checkHeaderName(x, cfg.getReplaceHeaderSpecialCharactersWith())).collect(Collectors.toList());
    }

    /**
     * Generates a generic header if not is provided or inferred
     * @param columnCount Number of columns in this file
     * @return List of string headers
     */
    private List<String> generateGenericHeader(int columnCount) {
        LOG.info("Building a generic header");
        var header = new ArrayList<String>();

        for (var i = 1; i <= columnCount; i++) {
            header.add("COLUMN_" + i);
        }

        return header;
    }

    /**
     * Processes a CSV file
     * @param fileName Name of the CSV file
     * @param bin BufferedInputStream for reading
     * @param c Current process context
     * @throws IOException for IO errors when reading
     */
    @Override
    protected void processFile(String fileName, BufferedInputStream bin, ProcessContext c) throws IOException {
        var cfg = (ReadCSVStep) this.getConfig();
        var format = cfg.getCSVFormat();

        LOG.info("Processing CSV file: {}", fileName);
        LOG.debug("CSV Format: {}", format);

        if (cfg.getDynamicDestinationConfig() != null) {
            this.setOutputTable(cfg.getDynamicDestinationConfig().getTableName(fileName));
        }

        this.addMetadata(ReadFileStep.FILE_NAME_FIELD, fileName.replaceFirst("^\\./", ""));

        var parser = CSVParser.parse(bin, StandardCharsets.UTF_8, format);
        var records = parser.getRecords();

        var parsedHeader = parser.getHeaderNames();
        var headers = this.getHeaders(cfg, parsedHeader, records.size(), fileName);
        var schema = this.generateSchema(headers);

        c.output(this.getSchemaTag(), Map.of(this.getOutputTable(), schema));

        // build and output the rows
        for (var r: records) {
            var rowBuilder = Row.withSchema(schema);

            for (String rowValue : r.values()) {
                rowBuilder.addValues(rowValue);
            }

            c.output(this.getRecordsTag(), rowBuilder.build());
        }
    }
}



