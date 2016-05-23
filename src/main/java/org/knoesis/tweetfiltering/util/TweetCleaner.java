package org.knoesis.tweetfiltering.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TweetCleaner {

        private static String url = "((http|https):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)|(@\\w*|\\bRT)";

        public static String cleanTweets(String tweet) {
                Pattern pattern = Pattern.compile(url, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(tweet);

                while(matcher.find()) {
                        tweet = matcher.replaceAll("").trim();
                }

                return tweet;
        }

}