package org.ascension.addg.gcp.ingestion.read.file;

import org.ascension.addg.gcp.ingestion.core.StepMap;

@StepMap(name = "ReadJSON")
public class ReadJSONStep extends ReadFileStep {
    public ReadJSONStep() {
        super("org.ascension.addg.gcp.ingestion.read.file.ReadJSONDoFn");
    }
}
