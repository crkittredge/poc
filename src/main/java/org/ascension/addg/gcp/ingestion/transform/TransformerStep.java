package org.ascension.addg.gcp.ingestion.transform;

import org.ascension.addg.gcp.ingestion.core.BaseStep;

public abstract class TransformerStep extends BaseStep {

    protected TransformerStep(String implementation) {
        super(implementation);
    }
}
