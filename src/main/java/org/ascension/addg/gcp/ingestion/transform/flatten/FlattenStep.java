package org.ascension.addg.gcp.ingestion.transform.flatten;

import org.ascension.addg.gcp.ingestion.core.StepMap;
import org.ascension.addg.gcp.ingestion.transform.TransformerStep;

/**
 * Configuration bean for InjectMetadata step
 */
@StepMap(name = "Flatten")
public class FlattenStep extends TransformerStep {

    /**
     * Instantiates a new Flatten step configuration with a custom implementation
     * @param implementation implementation class name
     */
    public FlattenStep(String implementation) {
        super(implementation);
    }

    /**
     * Instantiates a new Metadata step configuration
     */
    public FlattenStep() {
        this("org.ascension.addg.gcp.ingestion.transform.flatten.FlattenDoFn");
    }

}
