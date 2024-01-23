package org.ascension.addg.gcp.ingestion.read.bigquery;

import com.google.api.services.bigquery.model.TableReference;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.lang3.ObjectUtils;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;
import org.ascension.addg.gcp.ingestion.read.ReadStep;
import org.ascension.addg.gcp.ingestion.read.Reader;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.util.List;
import java.util.Objects;

/**
 * Implementation of a BigQuery Reader
 */
public final class BigQueryReader extends Reader<PBegin> {

    /**
     * Instantiates a new object
     * @param config Step configuration
     * @param options Pipeline options
     */
    public BigQueryReader(ReadStep config, IngestionOptions options) {
        super(config, options);

        var cfg = (ReadBigQueryStep) this.getConfig();

        // validate either a query or combination datasetId/tableId were provided
        if (cfg.getQuery() == null && ObjectUtils.anyNull(cfg.getProjectId(), cfg.getDatasetId(), cfg.getTableId())) {
            throw new BigQueryReader.ConfigurationException("Either query or projectId/datasetId/tableId must be provided");
        }
    }

    /**
     * Raised for an invalid configuration
     */
    public static class ConfigurationException extends RuntimeException {
        public ConfigurationException(String m) {
            super(m);
        }
    }

    /**
     * Converts a SchemaAndRecord to a beam Row
     */
    public static class SchemaAndRecordToRow implements SerializableFunction<SchemaAndRecord, Row> {
        public Row apply(SchemaAndRecord schemaAndRecord) {
            return BigQueryUtils.toBeamRow(Objects.requireNonNull(schemaAndRecord).getRecord(),
                    BigQueryUtils.fromTableSchema(schemaAndRecord.getTableSchema()),
                    BigQueryUtils.ConversionOptions.builder().build());
        }
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized PCollectionTuple expand(@NonNull PBegin input) {
        var cfg = (ReadBigQueryStep) this.getConfig();
        PCollectionTuple result;

        var readObj = BigQueryIO.read(new BigQueryReader.SchemaAndRecordToRow()).usingStandardSql();

        // get query results
        if (cfg.getQuery() != null) {
            readObj = readObj.fromQuery(cfg.getQuery());

        // or read from table
        } else {
            readObj = readObj.from(new TableReference()
                    .setProjectId(cfg.getProjectId())
                    .setDatasetId(cfg.getDatasetId())
                    .setTableId(cfg.getTableId()));
        }

        result = input.apply("Read from BigQuery", readObj)
                .apply("Parse Messages",
                        ParDo.of(cfg.<ReadBigQueryDoFn>getImplementationObj(this.getRecordsTag(), this.getBadRecordsTag(), this.getSchemaTag(), this.getBadSchemaTag()))
                                .withOutputTags(this.getRecordsTag(), TupleTagList.of(List.of(this.getBadRecordsTag(), this.getSchemaTag(), this.getBadSchemaTag()))));

        result = PCollectionTuple
                .of(this.getRecordsTag(), result.get(this.getRecordsTag()).setCoder(SerializableCoder.of(Row.class)))
                .and(this.getBadRecordsTag(), result.get(this.getBadRecordsTag()).setCoder(SerializableCoder.of(Row.class)))
                .and(this.getSchemaTag(), result.get(this.getSchemaTag()).setCoder(MapCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Schema.class))))
                .and(this.getBadSchemaTag(), result.get(this.getBadSchemaTag()).setCoder(MapCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Schema.class))));

        return result;
    }

}
