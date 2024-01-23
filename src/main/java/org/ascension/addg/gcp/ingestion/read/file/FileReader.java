package org.ascension.addg.gcp.ingestion.read.file;

import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.commons.lang3.StringUtils;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;
import org.ascension.addg.gcp.ingestion.core.Reshuffle;
import org.ascension.addg.gcp.ingestion.core.Utils;
import org.ascension.addg.gcp.ingestion.read.Reader;
import org.ascension.addg.gcp.ingestion.read.ReadStep;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.util.List;
import java.util.Objects;

/**
 * Reader implementation for files
 */
public final class FileReader extends Reader<PBegin> {

    /**
     * Instantiates a new FileReader object
     * @param config Step configuration
     * @param options Pipeline options
     */
    public FileReader(ReadStep config, IngestionOptions options) {
        super(config, options);
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized PCollectionTuple expand(@NonNull PBegin input) {
        var cfg = (ReadFileStep) this.getConfig();
        var opt = (FileIngestionOptions) this.options;
        var filePattern = "";
        PCollectionTuple result;

        if (opt.getPatternsFromFile().isAccessible() && !StringUtils.isEmpty(opt.getPatternsFromFile().get())) {
            filePattern = Utils.readFromGCS(opt.getPatternsFromFile()).get();
        } else if (opt.getInputFilePattern().isAccessible()) {
            filePattern = opt.getInputFilePattern().get();
        }

        LOG.info("File pattern: {}", filePattern);

        var files = input
                .apply("File List", Create.of(List.of(Objects.requireNonNull(filePattern).split("~"))))
                .apply("Match Input Files", FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW_IF_WILDCARD))
                .apply("Read Matched Files", FileIO.readMatches().withCompression(cfg.getCompressionFormat().getType()));

        // compressed files
        if (!cfg.getCompressionFormat().equals(ReadFileStep.CompressionFormat.NONE)) {
            result = files
                    .apply("Read Archive Entries", ParDo.of(ReadArchiveDoFn.getInstance(cfg)))
                    .apply("Reshuffle Archive Entries", new Reshuffle<>())
                    .apply("Read Files",
                            ParDo.of(cfg.<ReadFileDoFn<KV<String, FileIO.ReadableFile>>>getImplementationObj(this.getRecordsTag(), this.getBadRecordsTag(), this.getSchemaTag(), this.getBadSchemaTag()))
                            .withOutputTags(this.getRecordsTag(), TupleTagList.of(List.of(this.getBadRecordsTag(), this.getSchemaTag(), this.getBadSchemaTag()))));

        // uncompressed files
        } else {
            result = files.apply("Read Files",
                    ParDo.of(cfg.<ReadFileDoFn<FileIO.ReadableFile>>getImplementationObj(this.getRecordsTag(), this.getBadRecordsTag(), this.getSchemaTag(), this.getBadSchemaTag()))
                    .withOutputTags(this.getRecordsTag(), TupleTagList.of(List.of(this.getBadRecordsTag(), this.getSchemaTag(), this.getBadSchemaTag()))));
        }

        result = PCollectionTuple
                .of(this.getRecordsTag(), result.get(this.getRecordsTag()).setCoder(SerializableCoder.of(Row.class)))
                .and(this.getBadRecordsTag(), result.get(this.getBadRecordsTag()).setCoder(SerializableCoder.of(Row.class)))
                .and(this.getSchemaTag(), result.get(this.getSchemaTag()).setCoder(MapCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Schema.class))))
                .and(this.getBadSchemaTag(), result.get(this.getBadSchemaTag()).setCoder(MapCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Schema.class))));

        return result;
    }
}
