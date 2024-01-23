package org.ascension.addg.gcp.ingestion.transform.metadata;

import com.typesafe.config.Optional;
import org.ascension.addg.gcp.ingestion.core.StepMap;
import org.ascension.addg.gcp.ingestion.transform.TransformerStep;

import java.io.Serializable;
import java.util.List;

/**
 * Configuration bean for InjectMetadata step
 */
@StepMap(name = "InjectMetadata")
public class MetadataStep extends TransformerStep {
    @Optional private String loadTimestampFieldName;
    @Optional private List<FieldRenameMapping> fieldsToRename;

    /**
     * Defines a field renaming configuration
     */
    public static class FieldRenameMapping implements Serializable {
        private String fromField;
        private String toField;

        /**
         * Instantiates a FieldRenameMapping object
         */
        public FieldRenameMapping() {
            // do nothing
        }

        /**
         * Return the name of the from field
         * @return String from field
         */
        public final String getFromField() {
            return this.fromField;
        }

        /**
         * Set the from field
         * @param fromField String from field
         */
        public final void setFromField(String fromField) {
            this.fromField = fromField;
        }

        /**
         * Return the name of the to field
         * @return String from field
         */
        public final String getToField() {
            return this.toField;
        }

        /**
         * Set the to field
         * @param toField String to field
         */
        public final void setToField(String toField) {
            this.toField = toField;
        }
    }

    /**
     * Instantiates a new Metadata step configuration with a custom implementation
     * @param implementation implementation class name
     */
    public MetadataStep(String implementation) {
        super(implementation);
        this.fieldsToRename = List.of();
        this.loadTimestampFieldName = "meta_load_timestamp";
    }

    /**
     * Instantiates a new Metadata step configuration
     */
    public MetadataStep() {
        this("org.ascension.addg.gcp.ingestion.transform.metadata.MetadataDoFn");
    }

    /**
     * Returns a list of FieldRenameMappings
     * FieldRenameMappings
     */
    public final List<FieldRenameMapping> getFieldsToRename() {
        return this.fieldsToRename;
    }

    /**
     * Sets the value of fieldsToRename
     * @param fieldsToRename List of FieldRenameMapping
     */
    public final void setFieldsToRename(List<FieldRenameMapping> fieldsToRename) {
        this.fieldsToRename = fieldsToRename;
    }

    /**
     * Returns the value for loadTimestampFieldName
     * @return String loadTimestampFieldName
     */
    public String getLoadTimestampFieldName() {
        return loadTimestampFieldName;
    }

    /**
     * Sets the value for loadTimestampFieldName
     * @param loadTimestampFieldName String loadTimestampFieldName
     */
    public void setLoadTimestampFieldName(String loadTimestampFieldName) {
        this.loadTimestampFieldName = loadTimestampFieldName;
    }
}
