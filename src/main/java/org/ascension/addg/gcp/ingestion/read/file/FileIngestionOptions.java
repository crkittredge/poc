package org.ascension.addg.gcp.ingestion.read.file;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.commons.lang3.StringUtils;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;

/**
 * Defines pipeline options for file-based ingestion
 */
public interface FileIngestionOptions extends IngestionOptions {

    /**
     * Returns the input file pattern
     * @return ValueProvider of String
     */
    @Description("The file pattern to read records from (e.g. gs://bucket/data_file-*.csv)")
    @Default.String(StringUtils.EMPTY)
    ValueProvider<String> getInputFilePattern();

    /**
     * Sets the input file pattern
     * @param value String file pattern
     */
    void setInputFilePattern(ValueProvider<String> value);

    /**
     * The file path to a reference file to read file patterns from (e.g. gs://bucket/file_patterns-*.txt)
     * @return ValueProvider of String
     */
    @Description("The file pattern for the reference file to read file patterns from (e.g. gs://bucket/file_patterns-*.txt)")
    @Default.String(StringUtils.EMPTY)
    ValueProvider<String> getPatternsFromFile();

    /**
     * Sets the file path to the pattern file
     * @param value String file path
     */
    void setPatternsFromFile(ValueProvider<String> value);

}