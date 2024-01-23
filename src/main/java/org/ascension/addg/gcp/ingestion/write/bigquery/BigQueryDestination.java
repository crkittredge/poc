package org.ascension.addg.gcp.ingestion.write.bigquery;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.ascension.addg.gcp.ingestion.write.Writer;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Implements a dynamic BigQuery destination
 */
public class BigQueryDestination extends DynamicDestinations<Row, String> {
    private final ValueProvider<String> projectId;
    private final ValueProvider<String> datasetId;
    private final WriteBigQueryStep config;
    private final PCollectionView<Map<String, Schema>> schemaSideInput;
    private final String defaultTableId;

    /**
     * Creates a BigQueryDestination instance
     * @param projectId GCP project ID
     * @param datasetId GCP dataset ID
     * @param config step configuration
     * @param schemaSideInput side input containing table->schema mappings
     * @param defaultTableId default write table if not provided in the row metadata
     */
    public BigQueryDestination(ValueProvider<String> projectId,
                               ValueProvider<String> datasetId,
                               WriteBigQueryStep config,
                               PCollectionView<Map<String, Schema>> schemaSideInput,
                               String defaultTableId) {

        this.projectId = projectId;
        this.datasetId = datasetId;
        this.config = config;
        this.schemaSideInput = schemaSideInput;
        this.defaultTableId = defaultTableId;
    }

    /**
     * Return a list of available side inputs
     * @return List of side inputs
     */
    @Override
    @NonNull
    public List<PCollectionView<?>> getSideInputs() {
        return List.of(this.schemaSideInput);
    }

    /**
     * Returns the name of the destination table.
     * Uses the value provided in the row metadata 'internal_OutputTable' if available,
     * otherwise looks for the value in the write configuration.
     * @param element current row
     * @return bigquery table name
     */
    @Override
    public String getDestination(ValueInSingleWindow<Row> element) {
        return Writer.getDestination(Objects.requireNonNull(element).getValue(), this.defaultTableId);
    }

    /**
     * Creates a TableDestination object using a provided destination table
     * @param destination BigQuery table nam
     * @return TableDestination object
     */
    @Override
    @NonNull
    public TableDestination getTable(String destination) {
        TableDestination tblDestination;
        var tblRef = new TableReference().setProjectId(this.projectId.get()).setDatasetId(this.datasetId.get()).setTableId(destination);

        if (this.config.getPartitionBy() != null) {
            tblDestination = new TableDestination(tblRef, "Output data from dataflow",
                    new TimePartitioning()
                            .setType(this.config.getPartitionBy().getDataType())
                            .setField(this.config.getPartitionBy().getColumnName()));
        } else {
            tblDestination = new TableDestination(tblRef, "Output data from dataflow");
        }
        return tblDestination;
    }

    /**
     * Returns the TableSchema associated with the target table
     * @param destination BigQuery table name
     * @return TableSchema object
     */
    @Override
    public TableSchema getSchema(String destination) {
        var sideInput = Objects.requireNonNull(sideInput(this.schemaSideInput));
        var beamSchema = sideInput.containsKey(destination) ? sideInput.get(destination) : sideInput.get("default");
        return BigQueryUtils.toTableSchema(beamSchema);
    }
}

