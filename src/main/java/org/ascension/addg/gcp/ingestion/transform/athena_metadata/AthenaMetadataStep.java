package org.ascension.addg.gcp.ingestion.transform.athena_metadata;

import org.ascension.addg.gcp.ingestion.core.StepMap;
import org.ascension.addg.gcp.ingestion.transform.TransformerStep;

/**
 * Defines configuration for the InjectAthenaMetadata step
 */
@StepMap(name = "InjectAthenaMetadata")
public class AthenaMetadataStep extends TransformerStep {
    private String ministryLookupPath;

    /**
     * Instantiates a new configuration object
     */
    public AthenaMetadataStep() {
        super("org.ascension.addg.gcp.ingestion.transform.athena_metadata.AthenaMetadataDoFn");
    }

    /**
     * Returns the file path to the lookup file
     * @return String file path
     */
    public final String getMinistryLookupPath() {
        return this.ministryLookupPath;
    }

    /**
     * Sets the file path to the lookup file
     * @param ministryLookupPath String file path
     */
    public final void setMinistryLookupPath(String ministryLookupPath) {
        this.ministryLookupPath = ministryLookupPath;
    }
}
