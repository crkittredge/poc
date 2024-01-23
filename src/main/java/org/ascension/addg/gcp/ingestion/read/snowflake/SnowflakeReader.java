package org.ascension.addg.gcp.ingestion.read.snowflake;

import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTagList;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;
import org.ascension.addg.gcp.ingestion.core.Utils;
import org.ascension.addg.gcp.ingestion.read.ReadStep;
import org.ascension.addg.gcp.ingestion.read.Reader;
import org.ascension.addg.gcp.ingestion.read.ReaderDoFn;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

/**
 * Reads data from snowflake
 * See: https://beam.apache.org/documentation/io/built-in/snowflake/#general-usage-1
 */
public class SnowflakeReader extends Reader<PBegin> {

    private final SnowflakeIO.DataSourceConfiguration dataSourceConfiguration;

    public SnowflakeReader(ReadStep config, IngestionOptions options) {
        super(config, options);
        var cfg = (ReadSnowflakeStep) config;

        this.dataSourceConfiguration = SnowflakeIO.DataSourceConfiguration.create()
                .withUsernamePasswordAuth(
                        Utils.CredentialsHelper.getSecret(options.getProject(), cfg.getUsernameSecret()),
                        Utils.CredentialsHelper.getSecret(options.getProject(), cfg.getPasswordSecret()))
                .withServerName(cfg.getServerName())
                .withWarehouse(cfg.getWarehouseName())
                .withDatabase(cfg.getDatabaseName())
                .withSchema(cfg.getSchemaName())
                .withRole(cfg.getRole());
    }

    @Override
    public @NonNull PCollectionTuple expand(@NonNull PBegin input) {
        var cfg = (ReadSnowflakeStep) this.getConfig();

        var readTransform = SnowflakeIO.<Row>read()
                .withDataSourceConfiguration(this.dataSourceConfiguration)
                .fromQuery(cfg.getQuery())
                .withStagingBucketName(cfg.getStagingBucketName())
                .withStorageIntegrationName(cfg.getStorageIntegrationName())
                .withCsvMapper(new SnowflakeReader.CSVMapper())
                .withCoder(SerializableCoder.of(Row.class));

        var result = input.apply("Read from Snowflake", readTransform)
                .apply("Parse Messages",
                        ParDo.of(cfg.<ReaderDoFn<Row>>getImplementationObj(this.getRecordsTag(), this.getBadRecordsTag(), this.getSchemaTag(), this.getBadSchemaTag()))
                                .withOutputTags(this.getRecordsTag(), TupleTagList.of(List.of(this.getBadRecordsTag(), this.getSchemaTag(), this.getBadSchemaTag()))));

        return PCollectionTuple.of(this.getRecordsTag(), result.get(this.getRecordsTag()).setCoder(SerializableCoder.of(Row.class)))
                .and(this.getBadRecordsTag(), result.get(this.getBadRecordsTag()).setCoder(SerializableCoder.of(Row.class)))
                .and(this.getSchemaTag(), result.get(this.getSchemaTag()).setCoder(MapCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Schema.class))))
                .and(this.getBadSchemaTag(), result.get(this.getBadSchemaTag()).setCoder(MapCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Schema.class))));
    }

    private static class CSVMapper implements SnowflakeIO.CsvMapper<Row> {
        @Override
        public Row mapRow(String[] parts) {
            var sb = Schema.builder();
            for (var i = 1; i <= parts.length; i++) {
                sb = sb.addNullableStringField("field_" + i);
            }

            var options = Schema.Options.builder()
                    .setOption(ReadStep.OUTPUT_TABLE_FIELD, Schema.FieldType.STRING, "default")
                    .build();

            var schema = sb.setOptions(options).build();
            var row = Row.withSchema(schema);

            for (var p: parts) {
                row = row.addValue(p);
            }
            return row.build();
        }
    }
}
