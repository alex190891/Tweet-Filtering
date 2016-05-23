package org.knoesis.tweetfiltering.storm.models;

import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import twitter4j.Status;

@Entity
public class MappedStatus {

    private @Id ObjectId id;
    private final Status status;

    public MappedStatus(Status status) {
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }

}