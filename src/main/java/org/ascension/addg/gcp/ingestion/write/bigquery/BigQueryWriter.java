package org.ascension.addg.gcp.ingestion.write.bigquery;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;
import org.ascension.addg.gcp.ingestion.write.WriteStep;
import org.ascension.addg.gcp.ingestion.write.Writer;
import org.joda.time.Duration;

import java.util.HashSet;
import java.util.Map;

/**
 * Implementation of a BigQuery Writer
 */
public class BigQueryWriter extends Writer {

    private final ValueProvider<String> projectId;
    private final ValueProvider<String> datasetId;

    /**
     * Writes a PCollection of Row to BigQuery
     * @param config Typesafe config
     * @param options pipeline options
     * @param recordsTag TupleTag for good messages
     * @param badRecordsTag TupleTag for bad messages
     * @param schemaTag TupleTag for good message schema map
     * @param badSchemaTag TupleTag for bad message schema map
     */
    public BigQueryWriter(WriteStep config, IngestionOptions options, TupleTag<Row> recordsTag, TupleTag<Row> badRecordsTag, TupleTag<Map<String, Schema>> schemaTag, TupleTag<Map<String, Schema>> badSchemaTag) {
        super(config, recordsTag, badRecordsTag, schemaTag, badSchemaTag);

        var cfg = (WriteBigQueryStep) config;
        this.projectId = cfg.getBqLandProject() != null ? ValueProvider.StaticValueProvider.of(cfg.getBqLandProject()) : options.getBqLandProject();
        this.datasetId = cfg.getBqLandDataset() != null ? ValueProvider.StaticValueProvider.of(cfg.getBqLandDataset()) : options.getBqLandDataset();
    }

    /**
     * Writes a PCollection of Row to BigQuery
     * @param input PCollection of Row
     * @param schemaSideInput table->schema side input
     */
    @Override
    protected void writeRows(PCollection<Row> input, PCollectionView<Map<String, Schema>> schemaSideInput, boolean isErrorDestination) {
        var cfg = (WriteBigQueryStep) this.getConfig();
        var bqCreateDisposition = cfg.getCreateDisposition();
        var bqWriteDisposition = cfg.getWriteDisposition();
        var bqWriteMethod = cfg.getWriteMethod();

        var schemaUpdateSet = new HashSet<BigQueryIO.Write.SchemaUpdateOption>();
        schemaUpdateSet.add(BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_ADDITION);
        schemaUpdateSet.add(BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_RELAXATION);

        var writeObj = BigQueryIO.<Row>write()
                .withWriteDisposition(bqWriteDisposition)
                .withCreateDisposition(bqCreateDisposition)
                .withMethod(bqWriteMethod)
                .withFormatFunction(BigQueryUtils.toTableRow())
                .to(new BigQueryDestination(this.projectId, this.datasetId, (WriteBigQueryStep) this.getConfig(), schemaSideInput, cfg.getBqLandTable() + (isErrorDestination ? cfg.getBqErrorTableSuffix() : "")));

        // write-method specific options
        if (bqWriteMethod.equals(BigQueryIO.Write.Method.STORAGE_WRITE_API) || bqWriteMethod.equals(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE)) {
            writeObj = writeObj
                    .ignoreUnknownValues()
                    .withTriggeringFrequency(Duration.standardSeconds(60))
                    .withAutoSchemaUpdate(true)
                    .withSchemaUpdateOptions(schemaUpdateSet);

        } else if (bqWriteMethod.equals(BigQueryIO.Write.Method.STREAMING_INSERTS)) {
            writeObj = writeObj
                    .withExtendedErrorInfo()
                    .withSchemaUpdateOptions(schemaUpdateSet)
                    .withFailedInsertRetryPolicy(InsertRetryPolicy.alwaysRetry());

        // file loads
        } else {
            writeObj = writeObj.withSchemaUpdateOptions(schemaUpdateSet);
        }

        // write to BigQuery
        input.apply(writeObj);
    }
}
