package org.ascension.addg.gcp.ingestion.read.bigquery;

import com.typesafe.config.Optional;
import org.ascension.addg.gcp.ingestion.core.StepMap;
import org.ascension.addg.gcp.ingestion.read.ReadStep;

/**
 * Defines configuration for ReadBigQuery step
 */
@StepMap(name = "ReadBigQuery")
public class ReadBigQueryStep extends ReadStep {
    @Optional private String projectId;
    @Optional private String datasetId;
    @Optional private String tableId;
    @Optional private String query;

    /**
     * Instantiates a new step configuration object
     */
    public ReadBigQueryStep() {
        super("org.ascension.addg.gcp.ingestion.read.bigquery.ReadBigQueryDoFn", BigQueryReader.class);
    }

    public String getProjectId() {
        return this.projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getDatasetId() {
        return this.datasetId;
    }

    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }

    public String getTableId() {
        return this.tableId;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    public String getQuery() {
        return this.query;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}
