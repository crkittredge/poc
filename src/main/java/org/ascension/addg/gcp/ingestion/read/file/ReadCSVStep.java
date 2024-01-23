package org.ascension.addg.gcp.ingestion.read.file;

import com.typesafe.config.Optional;
import org.apache.commons.csv.CSVFormat;
import org.ascension.addg.gcp.ingestion.core.StepMap;

/**
 * Configuration for reading CSV files
 */
@StepMap(name = "ReadCSV")
public class ReadCSVStep extends ReadFileStep {
    @Optional private String delimiterFormat;           // see https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.html
    @Optional private String delimiterChar;             // character to delimit fields in the row
    @Optional private String escapeChar;                // character used to escape special characters
    @Optional private boolean hasHeader;                // does the file include a header row?
    @Optional private String quoteChar;                 // quote character used to contain field data
    @Optional private boolean ignoreEmptyLines;         // discard empty lines in the file?
    @Optional private boolean ignoreSurroundingSpaces;  // trim spaces for each field?
    @Optional private String nullString;                // value in the data to represent null
    @Optional private String recordSeparator;           // line terminator character
    @Optional private String commentMarker;             // comment line designation character

    public ReadCSVStep() {
        super("org.ascension.addg.gcp.ingestion.read.file.ReadCSVDoFn");
        this.delimiterFormat = "DEFAULT";
        this.hasHeader = false;
        this.ignoreEmptyLines = true;
        this.ignoreSurroundingSpaces = false;
        this.nullString = "";
    }

    /**
     * Defines supported delimited file formats
     */
    public enum CsvType {
        DEFAULT(CSVFormat.DEFAULT),
        RFC4180(CSVFormat.RFC4180),
        EXCEL(CSVFormat.EXCEL),
        MYSQL(CSVFormat.MYSQL),
        TDF(CSVFormat.TDF),
        POSTGRES_CSV(CSVFormat.POSTGRESQL_CSV),
        POSTGRES_TEXT(CSVFormat.POSTGRESQL_TEXT);

        private final CSVFormat format;

        CsvType(CSVFormat format) {
            this.format = format;
        }

        public CSVFormat getFormat() {
            return format;
        }
    }

    /**
     * Returns the value of delimiterFormat
     * @return String delimiterFormat
     */
    public final String getDelimiterFormat() {
        return this.delimiterFormat;
    }

    /**
     * Sets the value of delimiterFormat
     * @param delimiterFormat String delimiterFormat
     */
    public final void setDelimiterFormat(String delimiterFormat) {
        this.delimiterFormat = delimiterFormat;
    }

    /**
     * Returns the value of delimiterChar
     * @return String delimiterChar
     */
    public final String getDelimiterChar() {
        return this.delimiterChar;
    }

    /**
     * Sets the value of delimiterChar
     * @param delimiterChar String delimiterChar
     */
    public final void setDelimiterChar(String delimiterChar) {
        this.delimiterChar = delimiterChar;
    }

    /**
     * Returns the value of escapeChar
     * @return String escapeChar
     */
    public final String getEscapeChar() {
        return this.escapeChar;
    }

    /**
     * Sets the value of escapeChar
     * @param escapeChar String escapeChar
     */
    public final void setEscapeChar(String escapeChar) {
        this.escapeChar = escapeChar;
    }

    /**
     * Returns the value of hasHeader
     * @return boolean hasHeader
     */
    public final boolean getHasHeader() {
        return this.hasHeader;
    }

    /**
     * Sets the value of hasHeader
     * @param hasHeader boolean hasHeader
     */
    public final void setHasHeader(boolean hasHeader) {
        this.hasHeader = hasHeader;
    }

    /**
     * Returns the value of quoteChar
     * @return String quoteChar
     */
    public final String getQuoteChar() {
        return this.quoteChar;
    }

    /**
     * Sets the value of quoteChar
     * @param quoteChar String quoteChar
     */
    public final void setQuoteChar(String quoteChar) {
        this.quoteChar = quoteChar;
    }

    /**
     * Returns the value of ignoreEmptyLines
     * @return boolean ignoreEmptyLines
     */
    public final boolean getIgnoreEmptyLines() {
        return this.ignoreEmptyLines;
    }

    /**
     * Sets the value of ignoreEmptyLines
     * @param ignoreEmptyLines boolean ignoreEmptyLines
     */
    public final void setIgnoreEmptyLines(boolean ignoreEmptyLines) {
        this.ignoreEmptyLines = ignoreEmptyLines;
    }

    /**
     * Returns the value of ignoreSurroundingSpaces
     * @return boolean ignoreSurroundingSpaces
     */
    public final boolean getIgnoreSurroundingSpaces() {
        return this.ignoreSurroundingSpaces;
    }

    /**
     * Sets the value of ignoreSurroundingSpaces
     * @param ignoreSurroundingSpaces boolean ignoreSurroundingSpaces
     */
    public final void setIgnoreSurroundingSpaces(boolean ignoreSurroundingSpaces) {
        this.ignoreSurroundingSpaces = ignoreSurroundingSpaces;
    }

    /**
     * Returns the value of nullString
     * @return String nullString
     */
    public final String getNullString() {
        return this.nullString;
    }

    /**
     * Sets the value of nullString
     * @param nullString String nullString
     */
    public final void setNullString(String nullString) {
        this.nullString = nullString;
    }

    /**
     * Returns the value of recordSeparator
     * @return String recordSeparator
     */
    public final String getRecordSeparator() {
        return this.recordSeparator;
    }

    /**
     * Sets the value of recordSeparator
     * @param recordSeparator String recordSeparator
     */
    public final void setRecordSeparator(String recordSeparator) {
        this.recordSeparator = recordSeparator;
    }

    /**
     * Returns the value of commentMarker
     * @return String commentMarker
     */
    public final String getCommentMarker() {
        return this.commentMarker;
    }

    /**
     * Sets the value of commentMarker
     * @param commentMarker String commentMarker
     */
    public final void setCommentMarker(String commentMarker) {
        this.commentMarker = commentMarker;
    }

    /**
     * Creates a new CSVFormat object
     * @return CSVFormat object
     */
    public CSVFormat getCSVFormat() {
        var csvType = CsvType.valueOf(this.getDelimiterFormat()).getFormat();

        var csvBuilder = csvType.builder()
                .setIgnoreEmptyLines(this.getIgnoreEmptyLines())
                .setIgnoreSurroundingSpaces(this.getIgnoreSurroundingSpaces());

        if (this.getDelimiterChar() != null) {
            csvBuilder = csvBuilder.setDelimiter(this.getDelimiterChar());
        }

        if (this.getRecordSeparator() != null) {
            csvBuilder = csvBuilder.setRecordSeparator(this.getRecordSeparator());
        }

        if (this.getNullString() != null) {
            csvBuilder = csvBuilder.setNullString(this.getNullString());
        }

        if (this.getEscapeChar() != null) {
            csvBuilder = csvBuilder.setEscape(this.getEscapeChar().isEmpty() ? null : this.getEscapeChar().charAt(0));
        }

        if (this.getQuoteChar() != null) {
            csvBuilder = csvBuilder.setQuote(this.getQuoteChar().isEmpty() ? null : this.getQuoteChar().charAt(0));
        }

        if (this.getCommentMarker() != null) {
            csvBuilder = csvBuilder.setCommentMarker(this.getCommentMarker().isEmpty() ? null : this.getCommentMarker().charAt(0));
        }

        // only skip first row if "hasHeader"=true
        if (this.getHasHeader()) {
            csvBuilder = csvBuilder.setHeader().setSkipHeaderRecord(true);
        }

        return csvBuilder.build();
    }
}
