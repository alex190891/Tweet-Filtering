package org.knoesis.tweetfiltering.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.knoesis.tweetfiltering.Constant.Data;
import org.knoesis.tweetfiltering.Constant.Label;

import twitter4j.Status;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

public class ClustererInstanceFactory {
	
	 private final Instances dataset;
	 private final Configuration config;
	 private final String datasetName;
	 private final int datasetSize;
	 
	 public ClustererInstanceFactory(Configuration conf, int datasetSize) {
		 this.config = conf;
		 this.datasetSize = datasetSize;
		 this.datasetName = conf.getString("dataset-name", "dataset");
		 this.dataset = createDataset();
	 }
	 
	 private Instances createDataset() {
		 Attribute textAttribute = new Attribute(Data.TEXT, (List<String>) null);
		 
		 ArrayList<Attribute> attributes = new ArrayList<>(1);
		 attributes.add(textAttribute);
		 
		 Instances instances = new Instances(datasetName, attributes, 1);
		 return instances;
	 }
	 
	 public Instances getDataset() {
		 return dataset;
	 }
	 
	 public Instance createInstance(Status status) {
		 Instance instance = new DenseInstance(2);
		 Attribute attribute = dataset.attribute(Data.TEXT);
		 
		 instance.setValue(attribute, TweetCleaner.cleanTweets(status.getText()));
		 instance.setDataset(dataset);
		 
		 return instance;
	 }
	 
	 public Instances createInstances(List<Status> statusList) {
		 Instance instance = null;
		 
		 for(Status status : statusList) {
			 instance = createInstance(status);
			 dataset.add(instance);
		 }
		 
		 //dataset.setClassIndex(dataset.numAttributes() - 1);
		 
		 return dataset;
	 }
	 
	 public Instances createSingleInstances(Status status) {
		 Instance instance = null;
		 instance = createInstance(status);
		 dataset.add(instance);
		 return dataset;
	 }

}