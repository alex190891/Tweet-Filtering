package org.knoesis.tweetfiltering.storm.bolt.clusterer;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.knoesis.tweetfiltering.Constant.Field;
import org.knoesis.tweetfiltering.Constant.File;
import org.knoesis.tweetfiltering.Constant.Stream;
import org.knoesis.tweetfiltering.storm.models.ClusteredStatus;
import org.knoesis.tweetfiltering.util.ClustererInstanceFactory;
import org.knoesis.tweetfiltering.util.OptionSetter;
import org.knoesis.tweetfiltering.util.Reflector;
import org.knoesis.tweetfiltering.util.TupleHelpers;
import org.knoesis.tweetfiltering.util.WekaOperations;

import twitter4j.Status;
import weka.clusterers.Clusterer;
import weka.clusterers.FilteredClusterer;
import weka.clusterers.SimpleKMeans;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.OptionHandler;
import weka.filters.unsupervised.attribute.StringToWordVector;

public class ClustererBolt extends BaseBasicBolt {

    private static final Logger LOG = Logger.getLogger(ClustererBolt.class.getName());

    private Configuration conf;
    private FilteredClusterer filteredClusterer;
    private SimpleKMeans simpleKMeans;
    private OptionHandler options;
    private LinkedBlockingQueue<Status> clustererUpdateQueue;
    private LinkedBlockingQueue<ClusteredStatus> clusteredInstances;
    private ClustererInstanceFactory dataFactory;
    private ExecutorService executor;
    private ClusterUpdater updater;
    private static  AtomicBoolean opened;
    private StringToWordVector stringToWordVector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Field.INSTANCE));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        conf = new MapConfiguration(stormConf);
        clustererUpdateQueue = new LinkedBlockingQueue<>();
        clusteredInstances = new LinkedBlockingQueue<>();
        executor = Executors.newSingleThreadExecutor();
        updater = new ClusterUpdater();
        String className = conf.getString("clusterer.classname");
        opened = new AtomicBoolean(false);
        try {
			simpleKMeans = new SimpleKMeans();
			filteredClusterer = new FilteredClusterer();
			stringToWordVector = new StringToWordVector();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

        if (TupleHelpers.isTickTuple(input)) {
            executor.submit(updater);
            return;
        }

        if(opened.get()) {
        	opened.set(false);
        	List<ClusteredStatus> instanceList = new ArrayList<>();
        	int size = clusteredInstances.size();
            clusteredInstances.drainTo(instanceList, size);
            System.out.println(instanceList.size());
            collector.emit(new Values(instanceList));
        }

        Status status = (Status) input.getValueByField(Field.STATUS);
        clustererUpdateQueue.offer(status);

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
         Config config = new Config();
         config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 180);
         return config;
    }

    public class ClusterUpdater implements Runnable {

    	@Override
    	public void run() {
    		int size = clustererUpdateQueue.size();
    		int classValue;
    		List<Status> statusList = new ArrayList<>(size);
    		clustererUpdateQueue.drainTo(statusList, size);
    		dataFactory = new ClustererInstanceFactory(conf, size);
    		WekaOperations.writeToArff(dataFactory.createInstances(statusList), File.CLUSTER_TRAINING);
    		try {
				Instances trainingInstances = WekaOperations.readArffFile(File.CLUSTER_TRAINING);
				simpleKMeans.setNumClusters(5);
				filteredClusterer.setFilter(stringToWordVector);
				filteredClusterer.setClusterer(simpleKMeans);
				filteredClusterer.buildClusterer(trainingInstances);
				Date date = new Date();

				Iterator<Status> iterator = statusList.iterator();
				while (iterator.hasNext()) {
					Status status = (Status) iterator.next();
					dataFactory = new ClustererInstanceFactory(conf, 1);
					WekaOperations.writeToArff(dataFactory.createSingleInstances(status), File.CLUSTER_TEST);
					Instances testInstances = WekaOperations.readArffFile(File.CLUSTER_TEST);
					for(Instance instance : testInstances){
						classValue = filteredClusterer.clusterInstance(instance);
						ClusteredStatus clusteredStatus = new ClusteredStatus(status, classValue, date);
						clusteredInstances.offer(clusteredStatus);
					}

				}

				opened.set(true);

				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

    	}
    }

}
