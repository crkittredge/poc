package org.ascension.addg.gcp.ingestion;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.ascension.addg.gcp.ingestion.core.IngestionOptions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class IngestionOptionsTest {

    /**
     * Validates the pipeline options for generic ingestion
     */
    @Test public void validateOptions() {
        var optionsList = new ArrayList<String>();
        PipelineOptionsFactory.describe(Set.of(IngestionOptions.class)).stream()
                .filter(o -> o.getGroup().startsWith(IngestionOptions.class.getPackageName()))
                .forEach((m) -> optionsList.add(m.getName()));

        var expected = List.of("bq_land_dataset", "bq_land_project", "job_name_override", "pipeline_config", "wait_until_finish");

        assertEquals(expected, optionsList);
    }
}
