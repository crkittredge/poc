package org.ascension.addg.gcp.ingestion.write.avro;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;
import org.ascension.addg.gcp.ingestion.write.WriteStep;
import org.ascension.addg.gcp.ingestion.write.Writer;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static org.apache.beam.sdk.transforms.Contextful.fn;
import static org.apache.beam.sdk.transforms.Requirements.requiresSideInputs;

/**
 * Implements an Avro Writer
 */
public class AvroWriter extends Writer {
    private final String currentDateStr = LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    private final String loadId = UUID.randomUUID().toString();

    /**
     * Writes a PCollection of Row to AVRO
     * @param config Typesafe config
     * @param options Pipeline options
     * @param recordsTag TupleTag for good messages
     * @param badRecordsTag TupleTag for bad messages
     * @param schemaTag TupleTag for good message schema map
     * @param badSchemaTag TupleTag for bad message schema map
     */
    public AvroWriter(WriteStep config, IngestionOptions options, TupleTag<Row> recordsTag, TupleTag<Row> badRecordsTag, TupleTag<Map<String, Schema>> schemaTag, TupleTag<Map<String, Schema>> badSchemaTag) {
        super(config, recordsTag, badRecordsTag, schemaTag, badSchemaTag);
    }

    /**
     * Write rows to the target
     * @param input input PCollection of Row
     * @param schemaSideInput Side input table/schema mapping
     * @param isErrorDestination True for writing to the error table
     */
    @Override
    protected void writeRows(PCollection<Row> input, PCollectionView<Map<String, Schema>> schemaSideInput, boolean isErrorDestination) {
        var cfg = (WriteAvroStep) this.getConfig();

        var writeObj = FileIO.<String, Row>writeDynamic()
                .by((SerializableFunction<Row, String>) r -> Writer.getDestination(r, cfg.getOutputDirectory() + (isErrorDestination ? cfg.getErrorDirectorySuffix() : "")))
                .via(fn(r -> {
                    var schema = SerializableUtils.clone(Objects.requireNonNull(r).getSchema());
                    schema.setUUID(UUID.randomUUID());
                    var avroSchema = AvroUtils.toAvroSchema(schema);
                    return AvroUtils.toGenericRecord(Row.withSchema(schema).addValues(r.getValues()).build(), avroSchema);
                }), fn((t, c) -> AvroIO.sink(AvroUtils.toAvroSchema(Objects.requireNonNull(c.sideInput(schemaSideInput)).getOrDefault(t, Objects.requireNonNull(c.sideInput(schemaSideInput)).get("default")))), requiresSideInputs(schemaSideInput)))
                .to(cfg.getOutputRoot())
                .withNaming(fn(d -> new FileIO.Write.FileNaming() {
                    @Override
                    public @UnknownKeyFor @NonNull @Initialized String getFilename(@UnknownKeyFor @NonNull @Initialized BoundedWindow window, @UnknownKeyFor @NonNull @Initialized PaneInfo pane, @UnknownKeyFor @Initialized int numShards, @UnknownKeyFor @Initialized int shardIndex, @UnknownKeyFor @NonNull @Initialized Compression compression) {
                        return String.format("table=%s/meta_load_date=%s/%s-%s%s", d, AvroWriter.this.currentDateStr, shardIndex, AvroWriter.this.loadId, cfg.getFilenameSuffix());
                    }
                }))
                .withDestinationCoder(StringUtf8Coder.of());

        if (cfg.getNumShards() != null) {
            writeObj = writeObj.withNumShards(cfg.getNumShards());
        }

        input.apply(writeObj);
    }
}
