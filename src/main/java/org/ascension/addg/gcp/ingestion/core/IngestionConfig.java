package org.ascension.addg.gcp.ingestion.core;

import com.typesafe.config.Config;
import com.typesafe.config.Optional;
import org.ascension.addg.gcp.ingestion.read.file.ReadFileStep;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Defines an ingestion configuration
 */
public class IngestionConfig implements Serializable {
    private String name;
    @Optional private String pipelineOptionsClass;
    private List<BaseStep> steps;

    /**
     * Instantiates a new ingestion configuration
     */
    public IngestionConfig() {
        this.pipelineOptionsClass = "org.ascension.addg.gcp.ingestion.core.IngestionOptions";
    }

    /**
     * Returns the friendly name for this configuration
     * @return friendly configuration name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Sets the friendly name for this configuration
     * @param name friendly configuration name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns a List of Steps defined for this pipeline
     * @return List of Steps
     */
    public List<BaseStep> getSteps() {
        return this.steps;
    }

    /**
     * Parses a list of provided Steps for configuration
     * @param steps list of typesafe config objects
     * @throws BaseStep.UnknownStepException if a step is not a valid StepType
     */
    public void setSteps(List<Config> steps) throws BaseStep.UnknownStepException {
        var stepList = new ArrayList<BaseStep>();
        for (var s: steps) {
            var stepObj = BaseStep.of(s);

            // default to use file ingestion options for file pipelines
            if (stepObj instanceof ReadFileStep &&
                    this.pipelineOptionsClass.equals("org.ascension.addg.gcp.ingestion.core.IngestionOptions")) {
                this.pipelineOptionsClass = "org.ascension.addg.gcp.ingestion.read.file.FileIngestionOptions";
            }

            stepList.add(stepObj);
        }
        this.steps = stepList;
    }

    /**
     * Sets the pipeline options if not using the default
     * @param pipelineOptionsClass pipeline options class name. Must inherit from IngestionOptions
     */
    public void setPipelineOptionsClass(String pipelineOptionsClass) {
        this.pipelineOptionsClass = pipelineOptionsClass;
    }

    /**
     * Returns the pipeline options class for this configuration
     * @return pipeline Options
     */
    public String getPipelineOptionsClass() {
        return this.pipelineOptionsClass;
    }
}
