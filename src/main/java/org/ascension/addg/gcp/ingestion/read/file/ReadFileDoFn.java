package org.ascension.addg.gcp.ingestion.read.file;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.ascension.addg.gcp.ingestion.read.ReadStep;
import org.ascension.addg.gcp.ingestion.read.ReaderDoFn;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.*;

/**
 * Abstraction for reading files
 * @param <I> Type we are reading from
 */
public abstract class ReadFileDoFn<I> extends ReaderDoFn<I> {
    private final Map<String, String> metadata;

    /**
     * Instantiates a new DoFn
     * @param config step configuration
     * @param recordsTag TupleTag for good messages
     * @param badRecordsTag TupleTag for bad messages
     * @param schemaTag TupleTag for good message schema map
     * @param badSchemaTag TupleTag for bad message schema map
     */
    protected ReadFileDoFn(ReadFileStep config, TupleTag<Row> recordsTag, TupleTag<Row> badRecordsTag, TupleTag<Map<String, Schema>> schemaTag, TupleTag<Map<String, Schema>> badSchemaTag) {
        super(config, recordsTag, badRecordsTag, schemaTag, badSchemaTag);
        this.metadata = new LinkedHashMap<>();
    }

    /**
     * Processes a file
     * @param fileName Name of the file
     * @param bin BufferedInputStream for reading
     * @param c Current process context
     * @throws IOException for IO errors when reading
     */
    protected abstract void processFile(String fileName, BufferedInputStream bin, ProcessContext c) throws IOException;

    /**
     * Process each file record
     * @param c Current process context
     * @throws IOException for IO errors when reading
     */
    @ProcessElement
    public final void processElement(ProcessContext c) throws IOException {
        var input = c.element();
        var cfg = (ReadFileStep) this.getConfig();

        if (input instanceof KV && cfg.getCompressionFormat().equals(ReadFileStep.CompressionFormat.ZIP)) {
            this.processZip(c);
        } else if (input instanceof KV && cfg.getCompressionFormat().equals(ReadFileStep.CompressionFormat.TAR)) {
            this.processTar(c);
        } else {
            var fileName = Objects.requireNonNull(Objects.requireNonNull((FileIO.ReadableFile) c.element()).getMetadata().resourceId().getFilename());
            try (var bin = new BufferedInputStream(Channels.newInputStream(Objects.requireNonNull((FileIO.ReadableFile) c.element()).open()))) {
                this.processFile(fileName, bin, c);
            }
        }
    }

    /**
     * Creates an all-string beam Schema from a list of header names
     * @param headers List of string headers
     * @return beam Schema object
     */
    protected Schema generateSchema(List<String> headers) {
        // configure all-string schema
        var schemaBuilder = Schema.builder();

        for (var h : headers) {
            schemaBuilder = schemaBuilder.addNullableStringField(h).setOptions(this.getMetadataOptions());
        }

        return schemaBuilder.build();
    }

    /**
     * Processes each file inside a ZIP archive
     * @param c Current process context
     * @throws IOException for IO errors on read
     */
    @SuppressWarnings("unchecked")
    public void processZip(ProcessContext c) throws IOException {
        var input = Objects.requireNonNull((KV<String, FileIO.ReadableFile>) c.element());

        var fileName = input.getKey();
        var readableFile = Objects.requireNonNull(input.getValue());
        var zipFileName = Objects.requireNonNull(readableFile.getMetadata().resourceId().getFilename());

        try (var sbc = readableFile.openSeekable();
             var zipFile = new ZipFile(sbc)) {
            var entry = Objects.requireNonNull(zipFile.getEntry(fileName));

            this.addMetadata(ReadFileStep.ARCHIVE_FILE_NAME_FIELD, zipFileName);
            try (var bin = new BufferedInputStream(zipFile.getInputStream(entry))) {
                this.processFile(fileName, bin, c);
            }
        }
    }

    /**
     * Processes each file inside a TAR archive
     * @param c Current process context
     * @throws IOException for IO errors on read
     */
    @SuppressWarnings("unchecked")
    public void processTar(ProcessContext c) throws IOException {
        var input = Objects.requireNonNull((KV<String, FileIO.ReadableFile>) c.element());

        var fileName = Objects.requireNonNull(input.getKey());
        var readableFile = Objects.requireNonNull(input.getValue());
        var tarFileName = Objects.requireNonNull(readableFile.getMetadata().resourceId().getFilename());

        try (var sbc = input.getValue().open();
             var tin = new TarArchiveInputStream(Channels.newInputStream(sbc));
             var bin = new BufferedInputStream(tin)) {

            this.addMetadata(ReadFileStep.ARCHIVE_FILE_NAME_FIELD, tarFileName.replaceFirst("^\\./", ""));

            ArchiveEntry entry;

            while ((entry = tin.getNextEntry()) != null) {
                if (entry.getName().equalsIgnoreCase(input.getKey())) {
                    this.processFile(fileName, bin, c);
                }
            }
        }
    }

    /**
     * Returns the metadata for the row
     * @return Map of metadata
     */
    protected Map<String, String> getMetadata() {
        return this.metadata;
    }

    /**
     * Adds metadata for the row
     * @param key Metadata key
     * @param value Metadata value
     */
    protected void addMetadata(String key, String value) {
        this.metadata.put(key, value);
    }

    /**
     * Returns the metadata as beam Schema Options
     * @return Schema.Options object
     */
    protected Schema.Options getMetadataOptions() {
        var ob = Schema.Options.builder();
        for(var m: this.getMetadata().entrySet()) {
            ob = ob.setOption(m.getKey(), Schema.FieldType.STRING, m.getValue());
        }
        return ob.build();
    }

    /**
     * Returns the output table for this row for dynamic destinations
     * @return output table name
     */
    protected String getOutputTable() {
        return this.metadata.getOrDefault(ReadStep.OUTPUT_TABLE_FIELD, "default");
    }

    /**
     * Sets the output table for this row for dynamic destinations
     * @param outputTable String output table name
     */
    protected void setOutputTable(String outputTable) {
        this.metadata.put(ReadStep.OUTPUT_TABLE_FIELD, outputTable);
    }
}
