package org.ascension.addg.gcp.ingestion.read.snowflake;

import com.typesafe.config.Optional;
import org.ascension.addg.gcp.ingestion.core.StepMap;
import org.ascension.addg.gcp.ingestion.read.ReadStep;

/**
 * Defines configuration for ReadSnowflakeStep
 */
@StepMap(name = "ReadSnowflake")
public class ReadSnowflakeStep extends ReadStep {
    private String stagingBucketName;
    private String query;
    private String usernameSecret;
    private String passwordSecret;
    private String serverName;
    private String warehouseName;
    private String databaseName;
    private String schemaName;
    private String storageIntegrationName;
    @Optional private String role;

    /**
     * Instantiates a new configuration object
     */
    public ReadSnowflakeStep() {
        super("org.ascension.addg.gcp.ingestion.read.snowflake.ReadSnowflakeDoFn", SnowflakeReader.class);
        this.role = "PUBLIC";
    }

    /**
     * Returns the value of stagingBucketName
     * @return String stagingBucketName
     */
    public final String getStagingBucketName() {
        return stagingBucketName;
    }

    /**
     * Sets the value of stagingBucketName
     * @param stagingBucketName String value
     */
    public final void setStagingBucketName(String stagingBucketName) {
        this.stagingBucketName = stagingBucketName;
    }

    /**
     * Returns the value of query
     * @return String query
     */
    public final String getQuery() {
        return query;
    }

    /**
     * Sets the value of query
     * @param query String value
     */
    public final void setQuery(String query) {
        this.query = query;
    }

    /**
     * Returns the value of usernameSecret
     * @return String usernameSecret
     */
    public String getUsernameSecret() {
        return this.usernameSecret;
    }

    /**
     * Sets the value of usernameSecret
     * @param usernameSecret String value
     */
    public void setUsernameSecret(String usernameSecret) {
        this.usernameSecret = usernameSecret;
    }

    /**
     * Returns the value of passwordSecret
     * @return String passwordSecret
     */
    public String getPasswordSecret() {
        return this.passwordSecret;
    }

    /**
     * Sets the value of passwordSecret
     * @param passwordSecret String value
     */
    public void setPasswordSecret(String passwordSecret) {
        this.passwordSecret = passwordSecret;
    }

    /**
     * Returns the value of serverName
     * @return String serverName
     */
    public String getServerName() {
        return this.serverName;
    }

    /**
     * Sets the value of serverName
     * @param serverName String value
     */
    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    /**
     * Returns the value of warehouseName
     * @return String warehouseName
     */
    public String getWarehouseName() {
        return this.warehouseName;
    }

    /**
     * Sets the value of warehouseName
     * @param warehouseName String value
     */
    public void setWarehouseName(String warehouseName) {
        this.warehouseName = warehouseName;
    }

    /**
     * Returns the value of databaseName
     * @return String databaseName
     */
    public String getDatabaseName() {
        return this.databaseName;
    }

    /**
     * Sets the value of databaseName
     * @param databaseName String value
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Returns the value of schemaName
     * @return String schemaName
     */
    public String getSchemaName() {
        return this.schemaName;
    }

    /**
     * Sets the value of schemaName
     * @param schemaName String value
     */
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * Returns the value of storageIntegrationName
     * @return String storageIntegrationName
     */
    public String getStorageIntegrationName() {
        return this.storageIntegrationName;
    }

    /**
     * Sets the value of storageIntegrationName
     * @param storageIntegrationName String value
     */
    public void setStorageIntegrationName(String storageIntegrationName) {
        this.storageIntegrationName = storageIntegrationName;
    }

    /**
     * Returns the value of role
     * @return String role
     */
    public String getRole() {
        return role;
    }

    /**
     * Sets the value of role
     * @param role String value
     */
    public void setRole(String role) {
        this.role = role;
    }
}
