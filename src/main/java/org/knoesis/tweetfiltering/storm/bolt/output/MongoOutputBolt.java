package org.knoesis.tweetfiltering.storm.bolt.output;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.mongodb.BulkWriteOperation;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.knoesis.tweetfiltering.Constant.Field;
import org.knoesis.tweetfiltering.Constant.Stream;
import org.knoesis.tweetfiltering.storm.models.ClusteredStatus;
import org.knoesis.tweetfiltering.storm.models.MappedStatus;
import org.knoesis.tweetfiltering.util.MongoUtil;
import org.mongodb.morphia.Morphia;

import twitter4j.Status;

public class MongoOutputBolt extends BaseBasicBolt {

    private Configuration conf;
    private DB db;
    private DBCollection outputCollection;
    private DBCollection clusterCollection;
    private BulkWriteOperation bulkWriteOperation;
    private Morphia morphia;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        conf = new MapConfiguration(stormConf);
        morphia = new Morphia()
                .map(MappedStatus.class)
                .map(ClusteredStatus.class);
        this.db = MongoUtil.getDB(conf);
        this.outputCollection = db.getCollection(conf.getString("mongo.output-collection"));
        this.clusterCollection = db.getCollection(conf.getString("mongo.cluster-collection"));
        this.bulkWriteOperation = clusterCollection.initializeUnorderedBulkOperation();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (input.getSourceStreamId().equals(Stream.LABELED)) {
            Status status = (Status) input.getValueByField(Field.STATUS);
            DBObject statusDBObject = morphia.toDBObject(new MappedStatus(status));
            outputCollection.insert(statusDBObject);
        } else {
            List<ClusteredStatus> instanceList = (List<ClusteredStatus>) input.getValueByField(Field.INSTANCE);
            for (ClusteredStatus clusteredStatus : instanceList) {
                DBObject clusterStatus = morphia.toDBObject(clusteredStatus);
                bulkWriteOperation.insert(clusterStatus);
            }

            bulkWriteOperation.execute();
            this.bulkWriteOperation = clusterCollection.initializeUnorderedBulkOperation();
            System.out.println("Done");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}