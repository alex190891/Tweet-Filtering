package org.knoesis.tweetfiltering.storm.bolt.labeler;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.knoesis.tweetfiltering.Constant.Field;
import org.knoesis.tweetfiltering.Constant.Label;
import org.knoesis.tweetfiltering.Constant.Stream;

import twitter4j.Status;

public class StatusLabelerBolt extends BaseBasicBolt {

    private Configuration conf;
    private StatusLabeler labeler;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(Stream.LABELED, new Fields(Field.STATUS, Field.LABEL));
        declarer.declareStream(Stream.UNLABELED, new Fields(Field.STATUS));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        conf = new MapConfiguration(stormConf);
        List<String> positiveKeywords = Arrays.asList(conf.getStringArray("twitter.positive-keywords"));
        List<String> negativeKeywords = Arrays.asList(conf.getStringArray("twitter.negative-keywords"));
        this.labeler = new StatusLabeler(new HashSet<>(positiveKeywords), new HashSet<>(negativeKeywords));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
    	Status status = (Status) input.getValueByField(Field.STATUS);
        Label label = labeler.label(status.getText());
        if (label == Label.UNKNOWN) {
            collector.emit(Stream.UNLABELED, new Values(status));
        } else {
            collector.emit(Stream.LABELED, new Values(status, label));
        }
    }

}
