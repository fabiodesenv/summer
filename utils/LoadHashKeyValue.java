package br.edu.ufam.icomp.utils;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class LoadHashKeyValue {
	private static Logger logger = Logger.getLogger(LoadHashKeyValue.class);
	
	public static void main(String[] args) {
		if (args.length > 0) {
			Map<Long, String> result = load(args[0]);
			System.out.println("Finished loading: " + result.size());
			
			Long l = -6522066643276283100L;
			System.out.println( "Result: " + result.get(l) );
		}
		else
			System.err.println("Need argument.");
	}
	
	public static Map<Long, String> load(String pathToFile) {
		// struct to represent the key value structure
		HashMap<Long, String> result = new HashMap<Long, String>();
		
		long timeTI1 = System.currentTimeMillis();

		logger.info("Starting to load file " + pathToFile);
		
		FileInputStream inStream = null;
		DataInputStream inDataStream = null;
		BufferedReader br = null;
		

		System.out.println("\n\n-------------------------------------");
		System.out.println("Processing: " + pathToFile.toString());
		long time1 = System.currentTimeMillis();

		try {
			inStream = new FileInputStream(pathToFile);
		
			inDataStream = new DataInputStream(inStream);
			br = new BufferedReader(new InputStreamReader( inDataStream ));
	
			String str = null;
			long counter = 0;

			while ((str = br.readLine()) != null) {
				try {
					if (str.contains("6522066643276283100"))
						System.out.println("DEBUG");
					String[] splitted = str.split("\t");
	
					if (splitted.length>1) {
						if (splitted.length>2)
							result.put(Long.valueOf(splitted[0]), splitted[1] + "\t" + splitted[2]);
						else
							result.put(Long.valueOf(splitted[0]), splitted[1]);
					}
					else
						logger.error("Invalid line: " + str);
	
					counter++;

				} catch (NumberFormatException e ) {
					logger.error("Not possible to parse: " + str);
				}
			}
			long time2 = System.currentTimeMillis();
			System.out.println("Finished. Read " + counter + " lines. Took: " + (time2 - time1) + " ms");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				inStream.close();
				inDataStream.close();
				br.close();
			} catch(Exception e) {
				e.printStackTrace();
			}				
		}

		long timeTI2 = System.currentTimeMillis();
		logger.info("Time to load inverted index: " + (timeTI2 - timeTI1) + " ms");
		return result;
	}

}
