package org.ascension.addg.gcp.ingestion.read.file;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class FileIngestionOptionsTest {

    /**
     * Validates the pipeline options for file ingestion
     */
    @Test public void validateOptions() {
        var optionsList = new ArrayList<String>();
        PipelineOptionsFactory.describe(Set.of(FileIngestionOptions.class)).stream()
                .filter(o -> o.getGroup().startsWith(FileIngestionOptions.class.getPackageName()))
                .forEach((m) -> optionsList.add(m.getName()));

        var expected = List.of("input_file_pattern", "patterns_from_file");

        assertEquals(expected, optionsList);
    }
}
