package org.ascension.addg.gcp.ingestion.write.avro;

import com.typesafe.config.Optional;
import org.ascension.addg.gcp.ingestion.core.StepMap;
import org.ascension.addg.gcp.ingestion.write.WriteStep;

@StepMap(name = "WriteAvro")
public class WriteAvroStep extends WriteStep {
    @Optional private String filenameSuffix;        // optional suffix to append to output files
    @Optional private Integer numShards;            // value for manually sharding. Not recommended for batch pipelines, but required for streaming
    private String outputRoot;                      // output base location (e.g. gcs bucket folder)
    @Optional private String outputDirectory;       // output folder name (if not using dynamic destinations)
    @Optional private String errorDirectorySuffix;  // output folder name (if not using dynamic destinations)

    public WriteAvroStep() {
        super(null, AvroWriter.class);
        this.filenameSuffix = ".avro";
        this.outputDirectory = "default";
        this.errorDirectorySuffix = "_error";
    }

    /**
     * Returns the value of filenameSuffix
     * @return String filenameSuffix
     */
    public final String getFilenameSuffix() {
        return this.filenameSuffix;
    }

    public final void setFilenameSuffix(String filenameSuffix) {
        this.filenameSuffix = filenameSuffix;
    }

    /**
     * Returns the value of outputRoot
     * @return String outputRoot
     */
    public String getOutputRoot() {
        return this.outputRoot;
    }

    public void setOutputRoot(String outputRoot) {
        this.outputRoot = outputRoot;
    }

    /**
     * Returns the value of outputDirectory
     * @return String outputDirectory
     */
    public String getOutputDirectory() {
        return this.outputDirectory;
    }

    public void setOutputDirectory(String outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

    /**
     * Returns the value of numShards
     * @return Integer numShards
     */
    public Integer getNumShards() {
        return numShards;
    }

    public void setNumShards(Integer numShards) {
        this.numShards = numShards;
    }

    /**
     * Returns the value of errorDirectorySuffix
     * @return String errorDirectorySuffix
     */
    public String getErrorDirectorySuffix() {
        return this.errorDirectorySuffix;
    }

    public void setErrorDirectorySuffix(String errorDirectorySuffix) {
        this.errorDirectorySuffix = errorDirectorySuffix;
    }
}
