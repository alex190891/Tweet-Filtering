package org.knoesis.tweetfiltering.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.configuration.Configuration;
import weka.core.OptionHandler;

public class OptionSetter {

    private OptionSetter(){}

    public static void setOptions(OptionHandler options, Configuration conf) throws Exception {
        if (conf == null || conf.isEmpty()) {
            return;
        }
        List<String> optionList = new ArrayList<>();
        Iterator<String> keys = conf.getKeys();
        while (keys.hasNext()) {
            String key = keys.next();
            optionList.add("-" + key);
            String value = conf.getString(key);
            if (value != null && !value.isEmpty()) {
                optionList.add(value);
            }
        }
        options.setOptions(optionList.toArray(new String[optionList.size()]));
    }

}