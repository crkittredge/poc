package org.ascension.addg.gcp.ingestion.write;

import org.ascension.addg.gcp.ingestion.core.BaseStep;

public abstract class WriteStep extends BaseStep {
    private final Class<? extends Writer> writerClass;

    protected WriteStep(String implementation, Class<? extends Writer> writerClass) {
        super(implementation);
        this.writerClass = writerClass;
    }

    public final Class<? extends Writer> getWriterClass() {
        return this.writerClass;
    }
}
