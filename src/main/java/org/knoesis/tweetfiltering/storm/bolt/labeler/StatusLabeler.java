package org.knoesis.tweetfiltering.storm.bolt.labeler;

import java.util.Set;
import org.ahocorasick.trie.Trie;
import org.knoesis.tweetfiltering.Constant.Label;

public class StatusLabeler {

    private final Trie positive = new Trie().caseInsensitive();
    private final Trie negative = new Trie().caseInsensitive();

    public StatusLabeler(Set<String> positiveKeywords, Set<String> negativeKeywords) {
        for (String keyword : positiveKeywords) {
            positive.addKeyword(keyword);
        }
        for (String keyword : negativeKeywords) {
            negative.addKeyword(keyword);
        }
    }

    public Label label(String text) {
        boolean positiveMatch = !positive.parseText(text).isEmpty();
        boolean negativeMatch = !negative.parseText(text).isEmpty();
        if (!(positiveMatch ^ negativeMatch)) {
            return Label.UNKNOWN;
        } else if (positiveMatch) {
            return Label.POSITIVE;
        } else {
            return Label.NEGATIVE;
        }
    }
    
    public Label getClusteredStatusLabel(String label) {
    	if (label.equalsIgnoreCase("positive")) {
    		return Label.POSITIVE;
    	} else if (label.equalsIgnoreCase("negative")) {
    		return Label.NEGATIVE;
    	} else {
    		return Label.UNKNOWN;
    	}
    }

}

