package org.ascension.addg.gcp.ingestion.read.file;

import com.typesafe.config.Optional;
import org.apache.beam.sdk.io.Compression;
import org.ascension.addg.gcp.ingestion.read.ReadStep;

import java.io.Serializable;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Step config abstraction for reading files
 */
public abstract class ReadFileStep extends ReadStep {
    public static final String FILE_NAME_FIELD = "meta_file_name";
    public static final String ARCHIVE_FILE_NAME_FIELD = "meta_archive_file_name";
    @Optional private CompressionFormat compressionFormat;
    @Optional private List<String> skipFilesWithPattern;
    @Optional private List<HeaderPatternMapping> header;
    @Optional private DynamicDestinationConfig dynamicDestinationConfig;

    protected ReadFileStep(String implementation) {
        super(implementation, FileReader.class);
        this.compressionFormat = CompressionFormat.NONE;
        this.skipFilesWithPattern = List.of();
        this.header = List.of();
    }

    /**
     * Defines supported compression methods
     */
    public enum CompressionFormat {
        NONE(Compression.UNCOMPRESSED),
        ZIP(Compression.UNCOMPRESSED),
        TAR(Compression.AUTO);

        private final Compression type;

        CompressionFormat(Compression type) {
            this.type = type;
        }

        /**
         * Returns the Compression type associated with this CompressionFormat
         * @return Compression enum
         */
        public Compression getType() {
            return this.type;
        }
    }

    /**
     * Bean to define custom headers. Can optionally match headers to a given file pattern
     */
    public static class HeaderPatternMapping implements Serializable {
        @Optional private String filePattern;
        private List<String> columns;

        public HeaderPatternMapping() {
            this.filePattern = ".*";
        }

        public final String getFilePattern() {
            return this.filePattern;
        }

        public final void setFilePattern(String filePattern) {
            this.filePattern = filePattern;
        }

        public final List<String> getColumns() {
            return this.columns;
        }

        public final void setColumns(List<String> columns) {
            this.columns = columns;
        }
    }

    /**
     * Defines a dynamic destination configuration
     */
    public static class DynamicDestinationConfig implements Serializable {
        @Optional private String filenameParserRegex;
        @Optional private String tablePrefix;
        @Optional private String tableSuffix;
        @Optional private int patternMatchIndex;

        public DynamicDestinationConfig() {
            this.tablePrefix = "";
            this.tableSuffix = "";
            this.patternMatchIndex = 0;
            this.filenameParserRegex = ".*";
        }

        public String getFilenameParserRegex() {
            return this.filenameParserRegex;
        }

        public void setFilenameParserRegex(String filenameParserRegex) {
            this.filenameParserRegex = filenameParserRegex;
        }

        public String getTablePrefix() {
            return this.tablePrefix;
        }

        public void setTablePrefix(String tablePrefix) {
            this.tablePrefix = tablePrefix;
        }

        public String getTableSuffix() {
            return this.tableSuffix;
        }

        public void setTableSuffix(String tableSuffix) {
            this.tableSuffix = tableSuffix;
        }

        public int getPatternMatchIndex() {
            return this.patternMatchIndex;
        }

        public void setPatternMatchIndex(int patternMatchIndex) {
            this.patternMatchIndex = patternMatchIndex;
        }

        public String getTableName(String fileName) {
            var m = Pattern.compile(this.getFilenameParserRegex()).matcher(fileName);
            var table = m.find() ? m.group(this.getPatternMatchIndex()).toLowerCase() : fileName.toLowerCase();
            return String.format("%s%s%s", this.getTablePrefix(), table, this.getTableSuffix())
                    .replaceAll("\\W", "_")
                    .replaceAll("^_+", "")
                    .replaceAll("_+$", "")
                    .replaceAll("_{2,}", "_").trim().toLowerCase();
        }
    }

    public final CompressionFormat getCompressionFormat() {
        return this.compressionFormat;
    }

    public final void setCompressionFormat(CompressionFormat compressionFormat) {
        this.compressionFormat = compressionFormat;
    }

    public final List<String> getSkipFilesWithPattern() {
        return this.skipFilesWithPattern;
    }

    public final void setSkipFilesWithPattern(List<String> skipFilesWithPattern) {
        this.skipFilesWithPattern = skipFilesWithPattern;
    }

    public final List<HeaderPatternMapping> getHeader() {
        return this.header;
    }

    public final void setHeader(List<HeaderPatternMapping> header) {
        this.header = header;
    }

    public DynamicDestinationConfig getDynamicDestinationConfig() {
        return this.dynamicDestinationConfig;
    }

    public void setDynamicDestinationConfig(DynamicDestinationConfig dynamicDestinationConfig) {
        this.dynamicDestinationConfig = dynamicDestinationConfig;
    }
}
