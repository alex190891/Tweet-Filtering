package org.knoesis.tweetfiltering.storm.models;

import twitter4j.Status;
import weka.core.Instance;

public class MappedInstances {
	
	private Status status;
	private Instance instance;
	private int classValue;
	
	public MappedInstances(Status status) {
		this.status = status;
	}
	
	public MappedInstances(Status status, Instance instance) {
		this.status = status;
		this.instance = instance;
	}

	public Status getStatus() {
		return status;
	}

	public Instance getInstance() {
		return instance;
	}
	
	public void setClassValue(int classValue) {
		this.classValue = classValue;
	}
	
	public int getClassValue() {
		return classValue;
	}

}