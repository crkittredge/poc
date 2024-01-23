package org.ascension.addg.gcp.ingestion.core;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.Optional;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.atteo.classindex.ClassIndex;

import java.io.Serializable;
import java.util.Map;

/**
 * Top-level abstraction for a Step configuration
 */
public abstract class BaseStep implements Serializable {
    private String type;
    @Optional private String implementation;

    /**
     * Exception for when an unknown step is encountered
     */
    public static final class UnknownStepException extends Exception {

        /**
         * Instantiates a new exception for an invalid step
         * @param stepType provided step type
         */
        public UnknownStepException(String stepType) {
            super("Step type " + stepType + " is not valid");
        }
    }

    /**
     * Custom exception for when an invalid reflect implementation is encountered
     */
    public static final class InvalidImplementationException extends RuntimeException {

        /**
         * Instantiates a new exception
         * @param t Throwable to wrap
         */
        public InvalidImplementationException(Throwable t) {
            super(t);
        }
    }

    /**
     * Instantiates a new configuration object
     * @param implementation String DoFn implementation class
     */
    protected BaseStep(String implementation) {
        this.implementation = implementation;
    }

    /**
     * Returns an iterable of available Step classes
     * @return List of Class
     */
    public static Iterable<Class<?>> getStepClasses() {
        return ClassIndex.getAnnotated(StepMap.class);
    }

    /**
     * Returns the type of step
     * @return String
     */
    public final String getType() {
        return this.type;
    }

    /**
     * Sets the type of step
     * @param type String
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Instantiates the implementation associated with the step
     * @param recordsTag TupleTag for records
     * @param schema TupleTag for schema map
     * @return new object of type I
     * @param <I> class extending BaseDoFn
     */
    @SuppressWarnings("unchecked")
    public final <I extends BaseDoFn<?>> I getImplementationObj(TupleTag<Row> recordsTag, TupleTag<Map<String, Schema>> schema) {
        I retVal;
        try {
            var cls = (Class<? extends BaseDoFn<?>>) Class.forName(this.getImplementation());
            retVal = ((Class<I>)cls).getConstructor(this.getClass(), TupleTag.class, TupleTag.class)
                    .newInstance(this, recordsTag, schema);
        } catch (ReflectiveOperationException e) {
            throw new InvalidImplementationException(e);
        }
        return retVal;
    }

    /**
     * Returns the DoFn implementation associated with the step
     * @return String
     */
    public final String getImplementation() {
        return this.implementation;
    }

    /**
     * Sets the DoFn implementation associated with the step
     * @param implementation String
     */
    public final void setImplementation(String implementation) {
        this.implementation = implementation;
    }

    /**
     * Factor method for creating a new Step forma configuration
     * @param stepConfig Typesafe Config object
     * @return BaseStep
     * @throws UnknownStepException If the step was not found
     */
    public static BaseStep of(Config stepConfig) throws UnknownStepException {
        BaseStep retVal;
        var stepTypeName = stepConfig.getString("type").toUpperCase();
        Class<?> stepClass = null;

        for (var c: BaseStep.getStepClasses()) {
            if (c.getAnnotation(StepMap.class).name().toUpperCase().equals(stepTypeName)) {
                stepClass = c;
                break;
            }
        }

        if (stepClass != null) {
            retVal = (BaseStep) ConfigBeanFactory.create(stepConfig, stepClass);
        } else {
            throw new BaseStep.UnknownStepException(stepTypeName);
        }
        return retVal;
    }
}
