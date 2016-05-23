package org.knoesis.tweetfiltering.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.configuration.Configuration;
import org.knoesis.tweetfiltering.Constant.Data;
import org.knoesis.tweetfiltering.Constant.Label;
import twitter4j.Status;

import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

public class InstanceFactory {

    private final Instances dataset;
    private final Configuration config;
    private final String datasetName;
    private int dataSetSize;

    public InstanceFactory(Configuration conf, int dataSetSize) {
        this.config = conf;
        this.datasetName = conf.getString("dataset-name", "dataset");
        this.dataset = createDataset();
        this.dataSetSize = dataSetSize;
    }

    private Instances createDataset() {
        
        Attribute textAttribute = new Attribute(Data.TEXT, (List<String>) null);
        
        List<String> classes = Arrays.asList(Label.NEGATIVE.toString(), Label.POSITIVE.toString());
        Attribute classAttribute = new Attribute(Data.CLASS, classes);
        
        ArrayList<Attribute> attributes = new ArrayList<>(2);
        attributes.add(textAttribute);
        attributes.add(classAttribute);

        Instances instances = new Instances(datasetName, attributes, dataSetSize);
        instances.setClass(classAttribute);
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
    	
    	return dataset;
    }

}