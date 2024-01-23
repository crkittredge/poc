package org.ascension.addg.gcp.ingestion.read.mongodb;

import org.ascension.addg.gcp.ingestion.core.StepMap;
import org.ascension.addg.gcp.ingestion.read.ReadStep;

/**
 * Defines configuration for ReadMongoDb step
 */
@StepMap(name = "ReadMongoDb")
public class ReadMongoDbStep extends ReadStep {
    private String uri;
    private String database;
    private String collection;

    /**
     * Instantiates a new step configuration object
     */
    public ReadMongoDbStep() {
        super("org.ascension.addg.gcp.ingestion.read.mongodb.ReadMongoDbDoFn", MongoDbReader.class);
    }

    /**
     * Returns the value of uri
     * @return String uri
     */
    public final String getUri() {
        return this.uri;
    }

    /**
     * Sets the value of uri
     * @param uri String uri
     */
    public final void setUri(String uri) {
        this.uri = uri;
    }

    /**
     * Returns the value of database
     * @return String database
     */
    public final String getDatabase() {
        return this.database;
    }

    /**
     * Sets the value of database
     * @param database String database
     */
    public final void setDatabase(String database) {
        this.database = database;
    }

    /**
     * Returns the value of collection
     * @return String collection
     */
    public final String getCollection() {
        return this.collection;
    }

    /**
     * Sets the value of collection
     * @param collection Long collection
     */
    public final void setCollection(String collection) {
        this.collection = collection;
    }

}
