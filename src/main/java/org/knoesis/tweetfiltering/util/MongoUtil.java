package org.knoesis.tweetfiltering.util;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationRuntimeException;

public class MongoUtil {

    private static MongoClient CLIENT;

    private MongoUtil() {}

    public synchronized static DB getDB(Configuration conf) {
        if (CLIENT == null) {
            initClient(conf);
        }
        return CLIENT.getDB(conf.getString("mongo.database"));
    }

    private static void initClient(Configuration conf) {
        if (conf.containsKey("mongo.username") && conf.containsKey("mongo.password")) {
            MongoCredential cred = MongoCredential.createMongoCRCredential(conf.getString("mongo.username"),
                    conf.getString("mongo.database"), conf.getString("mongo.password").toCharArray());
            try {
                CLIENT = new MongoClient(new ServerAddress(conf.getString("mongo.host", "localhost"), conf.getInt("mongo.port", 27017)), Arrays.asList(cred));
            } catch (UnknownHostException ex) {
                throw new ConfigurationRuntimeException(ex);
            }
        } else {
            try {
                CLIENT = new MongoClient(conf.getString("mongo.host", "localhost"), conf.getInt("mongo.port", 27017));
            } catch (UnknownHostException ex) {
                throw new ConfigurationRuntimeException(ex);
            }
        }
    }

}
