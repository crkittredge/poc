package org.ascension.addg.gcp.ingestion.core;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * Defines common ingestion pipeline options
 */
public interface IngestionOptions extends DataflowPipelineOptions {

    /**
     * Returns the path to the configuration file
     * @return ValueProvider of String file path
     */
    @Description("Pipeline conf file GCS path")
    ValueProvider<String> getPipelineConfig();

    /**
     * Sets the path to the configuration file
     * @param value String file path
     */
    void setPipelineConfig(ValueProvider<String> value);

    /**
     * Returns the job name
     * @return ValueProvider of String job name
     */
    @Description("Override the Automatic Airflow Job Name ")
    ValueProvider<String> getJobNameOverride();

    /**
     * Sets the job name
     * @param value String job name
     */
    void setJobNameOverride(ValueProvider<String> value);

    /**
     * Returns the value of waitUntilFinish
     * @return ValueProvider of String waitUntilFinish
     */
    @Description("Wait for pipeline to complete")
    ValueProvider<String> getWaitUntilFinish();

    /**
     * Sets the value of waitUntilFinish
     * @param value String waitUntilFinish
     */
    void setWaitUntilFinish(ValueProvider<String> value);

    /**
     * Returns the BigQuery land project id
     * @return ValueProvider of String project id
     */
    @Description("GCP Project ID for BigQuery")
    ValueProvider<String> getBqLandProject();

    /**
     * Sets the BigQuery land project id
     * @param value String GCP project id
     */
    void setBqLandProject(ValueProvider<String> value);

    /**
     * Returns the BigQuery land dataset name
     * @return ValueProvider of String dataset name
     */
    @Description("BigQuery DataSet ID to write the output")
    ValueProvider<String> getBqLandDataset();

    /**
     * Sets the BigQuery land dataset name
     * @param value String dataset name
     */
    void setBqLandDataset(ValueProvider<String> value);
}