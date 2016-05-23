package org.knoesis.tweetfiltering.storm.spout.mongo;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.knoesis.tweetfiltering.Constant.Field;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.knoesis.tweetfiltering.Constant.Label;
import org.knoesis.tweetfiltering.util.MongoUtil;

//Look at the whole class once more...
public class UserLabeledSpout extends BaseRichSpout {

    private SpoutOutputCollector spoutOutputCollector;
    private LinkedBlockingQueue<DBObject> dbObjectQueue;
    private final AtomicBoolean opened = new AtomicBoolean(false);
    private Configuration configuration;
    private DB db;

    class TailableCursorThread extends Thread {

        LinkedBlockingQueue<DBObject> dbObjectQueue;
        DB db;

        public TailableCursorThread(LinkedBlockingQueue<DBObject> dbObjectQueue, DB db) {
            this.dbObjectQueue = dbObjectQueue;
            this.db = db;
        }

        @SuppressWarnings("deprecation")
        public void run() {

            while (opened.get()) {
                try {
                    db.requestStart();
                    final DBCursor cursor = MongoUtil.getDB(configuration)
                            .getCollection(configuration.getString("user-labeled-collection"))
                            .find();
                    try {
                        while (opened.get() && cursor.hasNext()) {
                            final DBObject doc = cursor.next();

                            if (doc == null) {
                                break;
                            }

                            dbObjectQueue.put(doc);
                        }
                    } finally {
                        try {
                            if (cursor != null) {
                                cursor.close();
                            }
                        } catch (final Throwable t) {
                        }
                        try {
                            db.requestDone();
                        } catch (final Throwable t) {
                        }
                    }

                    Utils.sleep(500);
                } catch (final MongoException.CursorNotFound cnf) {
                    if (opened.get()) {
                        throw cnf;
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    ;

    }

	@Override
    public void open(Map config, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        dbObjectQueue = new LinkedBlockingQueue<>();
        configuration = new MapConfiguration(config);

        try {
            db = new MongoClient(configuration.getString("mongo.host"), Integer.parseInt(configuration.getString("mongo.port"))).getDB(configuration.getString("mongo.database"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        TailableCursorThread mongoListener = new TailableCursorThread(dbObjectQueue, db);
        this.opened.set(true);
        mongoListener.start();
    }

    @Override
    public void nextTuple() {
        DBObject dbObject = this.dbObjectQueue.poll();

        if (dbObject == null) {
            Utils.sleep(50);
        } else {
            try {
                Status status = TwitterObjectFactory.createStatus(dbObject.get("status").toString());
                Label label = Label.valueOf(dbObject.get("label").toString());
                this.spoutOutputCollector.emit(new Values(status, label));
            } catch (TwitterException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Field.STATUS, Field.LABEL));
    }

}