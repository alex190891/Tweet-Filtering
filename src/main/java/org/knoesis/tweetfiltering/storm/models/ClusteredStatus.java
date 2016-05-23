package org.knoesis.tweetfiltering.storm.models;

import java.util.Date;

import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

import twitter4j.Status;

@Entity
public class ClusteredStatus {

    private @Id ObjectId id;
    private final int cluster;
    private final Status status;
    private Date date;

    public ClusteredStatus(Status status, int cluster, Date date) {
        this.status = status;
        this.cluster = cluster;
        this.date = date;
    }

    public Status getStatus() {
        return status;
    }

    public int getCluster() {
        return cluster;
    }

    public Date getDate() {
    	return date;
    }

}