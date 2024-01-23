package org.ascension.addg.gcp.ingestion.read.pubsub;

import com.typesafe.config.Optional;
import org.ascension.addg.gcp.ingestion.core.StepMap;
import org.ascension.addg.gcp.ingestion.read.ReadStep;

@StepMap(name = "ReadPubsub")
public class ReadPubsubStep extends ReadStep {
    @Optional private String topic;
    @Optional private String subscription;

    public ReadPubsubStep() {
        super("org.ascension.addg.gcp.ingestion.read.pubsub.ReadPubsubDoFn", PubsubReader.class);
    }

    public final String getTopic() {
        return topic;
    }

    public final void setTopic(String topic) {
        this.topic = topic;
    }

    public final String getSubscription() {
        return subscription;
    }

    public final void setSubscription(String subscription) {
        this.subscription = subscription;
    }
}
