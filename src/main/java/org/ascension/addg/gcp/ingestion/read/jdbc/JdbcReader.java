package org.ascension.addg.gcp.ingestion.read.jdbc;

import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;
import org.ascension.addg.gcp.ingestion.core.Utils;
import org.ascension.addg.gcp.ingestion.read.Reader;
import org.ascension.addg.gcp.ingestion.read.ReadStep;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.util.List;

/**
 * Implementation of a JDBC Reader
 */
public final class JdbcReader extends Reader<PBegin> {

    private final JdbcIO.DataSourceConfiguration dataSourceConfiguration;

    /**
     * Instantiates a new object
     * @param config Step configuration
     * @param options Pipeline options
     */
    public JdbcReader(ReadStep config, IngestionOptions options) {
        super(config, options);
        var cfg = (ReadJdbcStep) config;

        var dsc = JdbcIO.DataSourceConfiguration.create(cfg.getDriverClassName(), cfg.getConnectionURL());

        if (cfg.getUsernameSecret() != null) {
            dsc = dsc.withUsername(Utils.CredentialsHelper.getSecret(options.getProject(), cfg.getUsernameSecret()));
        }

        if (cfg.getPasswordSecret() != null) {
            dsc = dsc.withPassword(Utils.CredentialsHelper.getSecret(options.getProject(), cfg.getPasswordSecret()));
        }

        if (cfg.getConnectionProperties() != null) {
            dsc = dsc.withConnectionProperties(cfg.getConnectionProperties());
        }

        if (cfg.getDriverJars() != null) {
            dsc = dsc.withDriverJars(cfg.getDriverJars());
        }

        this.dataSourceConfiguration = dsc;
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized PCollectionTuple expand(@NonNull PBegin input) {
        var cfg = (ReadJdbcStep) this.getConfig();
        PCollectionTuple result;
        PTransform<@NonNull PBegin, @NonNull PCollection<Row>> readTransform;

        if (cfg.getPartitionColumn() != null) {
            var readObj = JdbcIO.<Row>readWithPartitions()
                    .withDataSourceConfiguration(this.dataSourceConfiguration)
                    .withTable(String.format("(%s) AS sq", cfg.getQuery()))
                    .withRowOutput()
                    .withPartitionColumn(cfg.getPartitionColumn());

            if (cfg.getLowerBound() != null) {
                readObj = readObj.withLowerBound(cfg.getLowerBound());
            }

            if (cfg.getUpperBound() != null) {
                readObj = readObj.withUpperBound(cfg.getUpperBound());
            }

            if (cfg.getNumPartitions() != null) {
                readObj = readObj.withNumPartitions(cfg.getNumPartitions());
            }

            readTransform = readObj;

        } else {
            readTransform = JdbcIO.readRows()
                    .withDataSourceConfiguration(this.dataSourceConfiguration)
                    .withQuery(cfg.getQuery())
                    .withFetchSize(cfg.getFetchSize())
                    .withOutputParallelization(true);
        }

        result = input.apply("Read from JDBC", readTransform)
                .apply("Parse Messages",
                        ParDo.of(cfg.<ReadJdbcDoFn>getImplementationObj(this.getRecordsTag(), this.getBadRecordsTag(), this.getSchemaTag(), this.getBadSchemaTag()))
                                .withOutputTags(this.getRecordsTag(), TupleTagList.of(List.of(this.getBadRecordsTag(), this.getSchemaTag(), this.getBadSchemaTag()))));

        result = PCollectionTuple
                .of(this.getRecordsTag(), result.get(this.getRecordsTag()).setCoder(SerializableCoder.of(Row.class)))
                .and(this.getBadRecordsTag(), result.get(this.getBadRecordsTag()).setCoder(SerializableCoder.of(Row.class)))
                .and(this.getSchemaTag(), result.get(this.getSchemaTag()).setCoder(MapCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Schema.class))))
                .and(this.getBadSchemaTag(), result.get(this.getBadSchemaTag()).setCoder(MapCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Schema.class))));

        return result;
    }

}
