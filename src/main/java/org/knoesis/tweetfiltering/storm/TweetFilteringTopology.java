package org.knoesis.tweetfiltering.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.knoesis.tweetfiltering.Constant.Bolt;
import org.knoesis.tweetfiltering.Constant.Spout;
import org.knoesis.tweetfiltering.Constant.Stream;
import org.knoesis.tweetfiltering.storm.bolt.classifier.ClassifierBolt;
import org.knoesis.tweetfiltering.storm.bolt.clusterer.ClustererBolt;
import org.knoesis.tweetfiltering.storm.bolt.labeler.StatusLabelerBolt;
import org.knoesis.tweetfiltering.storm.bolt.output.MongoOutputBolt;
import org.knoesis.tweetfiltering.storm.spout.mongo.UserLabeledSpout;
import org.knoesis.tweetfiltering.storm.spout.twitter.TwitterSpout;

public class TweetFilteringTopology {

    public static void main(String[] args) throws ClassNotFoundException, AlreadyAliveException, InvalidTopologyException, ConfigurationException {
        MapConfiguration conf = new MapConfiguration(new Config());
        try {
            conf.append(new XMLConfiguration(args[0]));
        } catch (ConfigurationException ex) {}

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(Spout.TWITTER_SPOUT, new TwitterSpout());
        builder.setSpout(Spout.USER_LABELED_SPOUT, new UserLabeledSpout());
        builder.setBolt(Bolt.LABELER_BOLT, new StatusLabelerBolt())
                .shuffleGrouping(Spout.TWITTER_SPOUT);
        builder.setBolt(Bolt.CLASSIFIER_BOLT, new ClassifierBolt())
                .shuffleGrouping(Bolt.LABELER_BOLT, Stream.LABELED)
                .shuffleGrouping(Bolt.LABELER_BOLT, Stream.UNLABELED)
                .shuffleGrouping(Spout.USER_LABELED_SPOUT);
        builder.setBolt(Bolt.CLUSTERER_BOLT, new ClustererBolt())
                .shuffleGrouping(Bolt.CLASSIFIER_BOLT, Stream.UNLABELED);
       builder.setBolt(Bolt.OUTPUT_BOLT, new MongoOutputBolt())
        		.shuffleGrouping(Bolt.CLASSIFIER_BOLT, Stream.LABELED)
        		.shuffleGrouping(Bolt.CLUSTERER_BOLT);

        String clusterMode = conf.getString(Config.STORM_CLUSTER_MODE);
        if ("local".equals(clusterMode)) {
            new LocalCluster().submitTopology(conf.getString(Config.TOPOLOGY_NAME, "active-filtering"), conf.getMap(), builder.createTopology());
        } else {
            StormSubmitter.submitTopology(conf.getString(Config.TOPOLOGY_NAME, "active-filtering"), conf.getMap(), builder.createTopology());
        }
    }

}
