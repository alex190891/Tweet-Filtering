package org.knoesis.tweetfiltering.storm.spout.twitter;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

public class TwitterStatusListener implements StatusListener {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterStatusListener.class);
    private final BlockingQueue<Status> queue;

    public TwitterStatusListener(BlockingQueue<Status> queue) {
        this.queue = queue;
    }

    @Override
    public void onStatus(Status status) {
        try {
            queue.put(status);
        } catch (InterruptedException ex) {
            LOG.error("Interrupted while queuing status", ex);
        }
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice sdn) {}

    @Override
    public void onTrackLimitationNotice(int i) {}

    @Override
    public void onScrubGeo(long l, long l1) {}

    @Override
    public void onStallWarning(StallWarning sw) {
        LOG.warn("StallWarning: " + sw.getMessage());
    }

    @Override
    public void onException(Exception ex) {
        LOG.error("Exception encountered during stream", ex);
    }

}
