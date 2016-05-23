package org.knoesis.tweetfiltering.storm.spout.twitter;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.knoesis.tweetfiltering.Constant.Field;
import twitter4j.FilterQuery;
import twitter4j.Status;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterSpout extends BaseRichSpout {

    private BlockingQueue<Status> queue;
    private TwitterStream stream;
    private Configuration conf;
    private SpoutOutputCollector collector;

    @Override
    public void open(Map stormConf, TopologyContext context, SpoutOutputCollector collector) {
        conf = new MapConfiguration(stormConf);
        this.collector = collector;
        queue = new LinkedBlockingQueue<>();
        ConfigurationBuilder streamConf = new ConfigurationBuilder()
                .setOAuthConsumerKey(conf.getString("twitter.consumer-key"))
                .setOAuthConsumerSecret(conf.getString("twitter.consumer-secret"))
                .setOAuthAccessToken(conf.getString("twitter.access-token"))
                .setOAuthAccessTokenSecret(conf.getString("twitter.access-token-secret"));
        stream = new TwitterStreamFactory(streamConf.build()).getInstance();
        stream.addListener(new TwitterStatusListener(queue));
        Set<String> keywords = new HashSet<>();
//        keywords.addAll(Arrays.asList(conf.getStringArray("twitter.positive-keywords")));
//        keywords.addAll(Arrays.asList(conf.getStringArray("twitter.negative-keywords")));
        keywords.addAll(Arrays.asList(conf.getStringArray("twitter.track-keywords")));
        FilterQuery query = new FilterQuery()
                .track(keywords.toArray(new String[keywords.size()]))
                .language(conf.getStringArray("twitter.track-languages"));
        stream.filter(query);
    }

    @Override
    public void close() {
        stream.clearListeners();
        stream.shutdown();
        stream.cleanUp();
        super.close();
    }

    @Override
    public void nextTuple() {
        Status status = queue.poll();
        if (status != null) {
            collector.emit(new Values(status));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Field.STATUS));
    }

}