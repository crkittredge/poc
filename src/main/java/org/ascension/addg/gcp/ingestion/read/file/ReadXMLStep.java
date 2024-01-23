package org.ascension.addg.gcp.ingestion.read.file;

import org.ascension.addg.gcp.ingestion.core.StepMap;

@StepMap(name = "ReadXML")
public class ReadXMLStep extends ReadFileStep {
    public ReadXMLStep() {
        super("org.ascension.addg.gcp.ingestion.read.file.ReadXMLDoFn");
    }
}
