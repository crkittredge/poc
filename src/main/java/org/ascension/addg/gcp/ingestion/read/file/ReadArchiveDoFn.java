package org.ascension.addg.gcp.ingestion.read.file;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Implementation for reading entries from an archive
 */
public class ReadArchiveDoFn extends DoFn<FileIO.ReadableFile, KV<String, FileIO.ReadableFile>> {

    /** logging object **/
    protected static final Logger LOG = LoggerFactory.getLogger(ReadArchiveDoFn.class);

    /** list of files to skip **/
    private final List<String> skipFilesList;
    private final ReadFileStep config;

    /**
     * Initializes an instance of this class
     * @param config step configuration
     */
    private ReadArchiveDoFn(ReadFileStep config) {
        this.config = config;
        this.skipFilesList = new ArrayList<>();
        this.skipFilesList.addAll(config.getSkipFilesWithPattern());

        // always exclude these
        this.skipFilesList.add("^.*audit.*$");
        this.skipFilesList.add("^.*readme.*$");
        this.skipFilesList.add("^\\./\\..*$");
    }

    /**
     * Factory method to instantiate this class
     * @param config Step configuration
     * @return new object instance
     */
    public static ReadArchiveDoFn getInstance(ReadFileStep config) {
        return new ReadArchiveDoFn(config);
    }

    /**
     * Process each file in this collection
     * @param c Current process context
     * @throws IOException for IO errors when reading the archive
     */
    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
        if (this.config.getCompressionFormat().equals(ReadFileStep.CompressionFormat.ZIP)) {
            this.extractZipEntries(c);
        } else {
            this.extractTarEntries(c);
        }
    }

    /**
     * Writes the member to output if not in the skip list
     * @param c Current process context
     * @param e Name of the entry
     */
    private void processEntry(ProcessContext c, String e) {
        var readableFile = Objects.requireNonNull(c.element());
        if (this.skipFilesList == null || this.skipFilesList.isEmpty() ||
                this.skipFilesList.stream().noneMatch(p -> Pattern.matches(p, e))) {
            LOG.info("Adding {} to list of entries to process.", e);
            c.output(KV.of(e, readableFile));
        } else {
            LOG.info("Skipping entry {}", e);
        }
    }

    /**
     * Reads the entries contained inside a ZIP file
     * @param c Current process context
     * @throws IOException for IO errors when reading
     */
    private void extractZipEntries(ProcessContext c) throws IOException {
        var readableFile = Objects.requireNonNull(c.element());

        var filename = Objects.requireNonNull(readableFile.getMetadata().resourceId().getFilename());
        var entries = new ArrayList<String>();

        if (filename.toLowerCase().endsWith(".zip")) {
            LOG.info("Extracting entries from {}", readableFile.getMetadata().resourceId().getFilename());
            try (var zips = new ZipInputStream(Channels.newInputStream(readableFile.open()))) {
                ZipEntry ze;
                while ((ze = zips.getNextEntry()) != null) {
                    entries.add(ze.getName());
                }
            }
        }

        entries.forEach(e -> this.processEntry(c, e));
    }

    /**
     * Reads the entries contained inside a TAR file
     * @param c Current process context
     * @throws IOException for IO errors when reading
     */
    private void extractTarEntries(ProcessContext c) throws IOException {
        var readableFile = Objects.requireNonNull(c.element());

        var filename = Objects.requireNonNull(readableFile.getMetadata().resourceId().getFilename());
        var entries = new ArrayList<String>();

        if (filename.toLowerCase().contains(".tar")) {
            LOG.info("Extracting entries from {}", readableFile.getMetadata().resourceId().getFilename());
            try (var tin = new TarArchiveInputStream(Channels.newInputStream(readableFile.open()))) {
                ArchiveEntry ae;
                while ((ae = tin.getNextEntry()) != null) {
                    entries.add(ae.getName());
                }
            }
        }

        entries.forEach(e -> this.processEntry(c, e));
    }
}

