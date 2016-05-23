package org.knoesis.tweetfiltering.storm.bolt.classifier;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.knoesis.tweetfiltering.Constant.Field;
import org.knoesis.tweetfiltering.Constant.Label;
import org.knoesis.tweetfiltering.Constant.Stream;
import org.knoesis.tweetfiltering.util.ClassifierInstanceFactory;
import org.knoesis.tweetfiltering.util.OptionSetter;
import org.knoesis.tweetfiltering.util.Reflector;
import org.knoesis.tweetfiltering.util.TupleHelpers;
import twitter4j.Status;
import weka.classifiers.Classifier;
import weka.classifiers.UpdateableBatchProcessor;
import weka.classifiers.UpdateableClassifier;
import weka.core.Instance;
import weka.core.OptionHandler;
import weka.core.Utils;

public class ClassifierBolt extends BaseRichBolt {

    private static final Logger LOG = Logger.getLogger(ClassifierBolt.class.getName());

    private Configuration conf;
    private Classifier classifier;
    private UpdateableClassifier updateable;
    private UpdateableBatchProcessor batch;
    private OptionHandler options;
    private double confidenceThreshold;
    private OutputCollector collector;
    private LinkedBlockingQueue<Instance> updateQueue;
    private ExecutorService executor;
    private Updater updater;
    private ClassifierInstanceFactory dataFactory;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(Stream.LABELED, new Fields(Field.STATUS));
        declarer.declareStream(Stream.UNLABELED, new Fields(Field.STATUS));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        conf = new MapConfiguration(stormConf);
        updateQueue = new LinkedBlockingQueue<>();
        updater = new Updater();
        dataFactory = new ClassifierInstanceFactory(conf);
        executor = Executors.newSingleThreadExecutor();
        this.collector = collector;
        confidenceThreshold = conf.getDouble("classifier.confidence-threshold");
        String className = conf.getString("classifier.classname");
        try {
            Reflector reflector = Reflector.create(className, new Object[]{},
                    Classifier.class,
                    UpdateableClassifier.class,
                    UpdateableBatchProcessor.class,
                    OptionHandler.class);
            classifier = reflector.get();
            updateable = reflector.get();
            batch = reflector.get();
            options = reflector.get();
            OptionSetter.setOptions(options, conf.subset("classifier.options"));
            classifier.buildClassifier(dataFactory.getDataset());
        } catch (Exception ex) {
            throw new RuntimeException("Failed to create classifier", ex);
        }
    }

    @Override
    public void execute(Tuple input) {

        if (TupleHelpers.isTickTuple(input)) {
            executor.submit(updater);
            return;
        }

        Status status = (Status) input.getValueByField(Field.STATUS);
        Instance instance = dataFactory.createInstance(status);
        if (input.getSourceStreamId().equals(Stream.LABELED)) {
            Label label = (Label) input.getValueByField(Field.LABEL);
            instance.setClassValue(label.ordinal());
            boolean inserted = updateQueue.offer(instance);
            if (inserted) {
                collector.ack(input);
                if (label == Label.POSITIVE) {
                    collector.emit(Stream.LABELED, new Values(status));
                }
            }
            LOG.log(Level.INFO, "Labeled {0}: {1}", new Object[]{label.toString(), status.getText()});
        } else {
            try {
                double prediction = classifier.classifyInstance(instance);
                double[] probabilities = classifier.distributionForInstance(instance);
                double maxConfidence = Math.max(probabilities[0], probabilities[1]);
                if (Utils.isMissingValue(prediction)) {
                    collector.fail(input);
                    return;
                }
                Label label = Label.valueOf(instance.classAttribute().value((int) prediction));
                LOG.log(Level.INFO, "Predicted {0} ({1}): {2}", new Object[]{label.toString(), maxConfidence, status.getText()});
                if (maxConfidence < confidenceThreshold) {
                    collector.emit(Stream.UNLABELED, new Values(status));
                } else {
                    if (label == Label.POSITIVE) {
                        collector.emit(Stream.LABELED, new Values(status));
                    }
                }
                collector.ack(input);
            } catch (Exception ex) {
                LOG.log(Level.WARNING, "Failed to classify instance", ex);
                collector.fail(input);
            }
        }

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
         Config config = new Config();
         config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 30);
         return config;
    }

    private final class Updater implements Runnable {
        @Override
        public void run() {
            int size = updateQueue.size();
            LOG.log(Level.INFO, "Classifier updater running with {0} instances", size);
            List<Instance> instances = new ArrayList<>(size);
            updateQueue.drainTo(instances, size);
            for (Instance instance : instances) {
                try {
                    updateable.updateClassifier(instance);
                } catch (Exception ex) {
                    LOG.log(Level.SEVERE, "Failed to add instance to update batch", ex);
                }
            }
            try {
                batch.batchFinished();
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "Failed to finish batch update", e);
            }
        }
    };

}

