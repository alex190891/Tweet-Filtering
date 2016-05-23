package org.knoesis.tweetfiltering.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import weka.core.Instances;

public class WekaOperations {
	
	public static void writeToArff(Instances instances, String fileName) {
		try {
			BufferedWriter b = new BufferedWriter(new FileWriter(new File(fileName)));
			b.write(instances.toString());
			b.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static Instances readArffFile(String fileName) throws FileNotFoundException, IOException {
		BufferedReader reader = new BufferedReader(new FileReader(new File(fileName)));
		Instances data = new Instances(reader);
		reader.close();
		return data;
		}

}