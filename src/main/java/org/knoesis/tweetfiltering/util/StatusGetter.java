package org.knoesis.tweetfiltering.util;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

public class StatusGetter {
	
	public static Status getStatus(String status) throws TwitterException {
		return TwitterObjectFactory.createStatus(status);
	}

}