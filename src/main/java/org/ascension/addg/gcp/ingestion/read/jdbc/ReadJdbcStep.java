package org.ascension.addg.gcp.ingestion.read.jdbc;

import com.typesafe.config.Optional;
import org.ascension.addg.gcp.ingestion.core.StepMap;
import org.ascension.addg.gcp.ingestion.read.ReadStep;

/**
 * Defines configuration for ReadJDBC step
 */
@StepMap(name = "ReadJDBC")
public class ReadJdbcStep extends ReadStep {
    private String connectionURL;
    @Optional private String usernameSecret;
    @Optional private String passwordSecret;
    private String driverClassName;
    @Optional private String driverJars;
    @Optional private String partitionColumn;
    @Optional private String connectionProperties;
    private String query;
    @Optional private Long lowerBound;
    @Optional private Long upperBound;
    @Optional private Integer numPartitions;
    @Optional private Integer fetchSize;

    /**
     * Instantiates a new step configuration object
     */
    public ReadJdbcStep() {
        super("org.ascension.addg.gcp.ingestion.read.jdbc.ReadJdbcDoFn", JdbcReader.class);
        this.numPartitions = 5;
        this.fetchSize = 1000;
    }

    /**
     * Returns the value of connectionURL
     * @return String connectionURL
     */
    public final String getConnectionURL() {
        return this.connectionURL;
    }

    /**
     * Sets the value of connectionURL
     * @param connectionURL String connectionURL
     */
    public final void setConnectionURL(String connectionURL) {
        this.connectionURL = connectionURL;
    }

    /**
     * Returns the value of usernameSecret
     * @return String usernameSecret
     */
    public final String getUsernameSecret() {
        return this.usernameSecret;
    }

    /**
     * Sets the value of usernameSecret
     * @param usernameSecret String usernameSecret
     */
    public final void setUsernameSecret(String usernameSecret) {
        this.usernameSecret = usernameSecret;
    }

    /**
     * Returns the value of lowerBound
     * @return Long lowerBound
     */
    public final Long getLowerBound() {
        return this.lowerBound;
    }

    /**
     * Sets the value of lowerBound
     * @param lowerBound Long lowerBound
     */
    public final void setLowerBound(Long lowerBound) {
        this.lowerBound = lowerBound;
    }

    /**
     * Returns the value of upperBound
     * @return Long upperBound
     */
    public final Long getUpperBound() {
        return this.upperBound;
    }

    /**
     * Sets the value of upperBound
     * @param upperBound Long upperBound
     */
    public final void setUpperBound(Long upperBound) {
        this.upperBound = upperBound;
    }

    /**
     * Returns the value of numPartitions
     * @return Integer numPartitions
     */
    public final Integer getNumPartitions() {
        return this.numPartitions;
    }

    /**
     * Sets the value of numPartitions
     * @param numPartitions Integer numPartitions
     */
    public final void setNumPartitions(Integer numPartitions) {
        this.numPartitions = numPartitions;
    }

    /**
     * Returns the value of fetchSize
     * @return Integer fetchSize
     */
    public final Integer getFetchSize() {
        return this.fetchSize;
    }

    /**
     * Sets the value of fetchSize
     * @param fetchSize Integer fetchSize
     */
    public final void setFetchSize(Integer fetchSize) {
        this.fetchSize = fetchSize;
    }

    /**
     * Returns the value of query
     * @return String query
     */
    public final String getQuery() {
        return this.query;
    }

    /**
     * Sets the value of query
     * @param query String query
     */
    public final void setQuery(String query) {
        this.query = query;
    }

    /**
     * Returns the value of passwordSecret
     * @return String passwordSecret
     */
    public final String getPasswordSecret() {
        return this.passwordSecret;
    }

    /**
     * Sets the value of passwordSecret
     * @param passwordSecret String passwordSecret
     */
    public final void setPasswordSecret(String passwordSecret) {
        this.passwordSecret = passwordSecret;
    }

    /**
     * Returns the value of driverClassName
     * @return String driverClassName
     */
    public final String getDriverClassName() {
        return this.driverClassName;
    }

    /**
     * Sets the value of driverClassName
     * @param driverClassName String driverClassName
     */
    public final void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }

    /**
     * Returns the value of partitionColumn
     * @return String partitionColumn
     */
    public final String getPartitionColumn() {
        return partitionColumn;
    }

    /**
     * Sets the value of partitionColumn
     * @param partitionColumn String partitionColumn
     */
    public final void setPartitionColumn(String partitionColumn) {
        this.partitionColumn = partitionColumn;
    }

    /**
     * Returns the value of connectionProperties
     * @return String connectionProperties
     */
    public final String getConnectionProperties() {
        return this.connectionProperties;
    }

    /**
     * Sets the value of connectionProperties
     * @param connectionProperties String connectionProperties
     */
    public final void setConnectionProperties(String connectionProperties) {
        this.connectionProperties = connectionProperties;
    }

    /**
     * Returns the value of driverJars
     * @return String driverJars
     */
    public final String getDriverJars() {
        return this.driverJars;
    }

    /**
     * Sets the value of driverJars
     * @param driverJars String driverJars
     */
    public final void setDriverJars(String driverJars) {
        this.driverJars = driverJars;
    }
}
