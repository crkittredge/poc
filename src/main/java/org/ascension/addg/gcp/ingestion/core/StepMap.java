package org.ascension.addg.gcp.ingestion.core;

import org.atteo.classindex.IndexAnnotated;

import java.lang.annotation.*;

/**
 * Use to map a step class to it's configuration name
 */
@IndexAnnotated
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
@Documented
public @interface StepMap {
    String name();
}