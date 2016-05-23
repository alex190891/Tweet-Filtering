package org.knoesis.tweetfiltering;

import org.knoesis.tweetfiltering.Constant.Conf;

public final class Constant {

    private Constant() {}

    public static final class Conf {
        public static final String PREFIX = Conf.class.getPackage().getName() + ".";
    }

    public static final class Data {
        public static final String TEXT = "text";
        public static final String CLASS = "class";
    }

    public static final class Spout {
        public static final String TWITTER_SPOUT = "twitter-spout";
        public static final String USER_LABELED_SPOUT = "user-labeled-spout";
    }

    public static final class Bolt {
        public static final String LABELER_BOLT = "labeler-bolt";
        public static final String CLASSIFIER_BOLT = "classifier-bolt";
        public static final String CLUSTERER_BOLT = "clusterer-bolt";
        public static final String OUTPUT_BOLT = "output-bolt";
    }

    public static final class Stream {
        public static final String LABELED = "labeled-stream";
        public static final String UNLABELED = "unlabeled-stream";
        public static final String CLUSTERED = "clustered-stream";
    }

    public static final class Field {
        public static final String STATUS = "status-field";
        public static final String LABEL = "label-field";
        public static final String CLUSTER = "cluster-field";
        public static final String INSTANCE = "instance-field";
    }

    public static final class File {
    	public static final String CLUSTER_TRAINING = "cluster-training";
    	public static final String CLUSTER_TEST = "cluster-test";
    }

    public static enum Label { NEGATIVE, POSITIVE, UNKNOWN };

}