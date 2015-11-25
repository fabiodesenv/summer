package br.edu.ufam.icomp.main;

import org.apache.log4j.Logger;

import br.edu.ufam.icomp.mr.IDF.IDFCalcDrive;
import br.edu.ufam.icomp.mr.WordCount.WordCountDrive3;


public class MapReduceMainDriver {
	private static Logger logger = Logger.getLogger(MapReduceMainDriver.class);
	public static int totaldocs = 0;
	public static final String tfSufix = "_TF_";
	public static final String idfSufix = "_IDF_";
	
	public static void main(String[] args){
		if ( (args.length < 2) || (args.length > 3)) {
			System.out.printf("Usage: MapReduceMainDriver <input> <output> [optionaly:number_reducers]\n");
			System.exit(-1);
		}

		String input = args[0];
		String output = args[1];
		
		int numReducers = 0;
		if (args.length==3)
			numReducers = Integer.parseInt(args[2]);
		
		MapReduceMainDriver main = new MapReduceMainDriver();
		main.run(input, output, numReducers);
	}
	
	void run(String input, String output, int numReducers){
		String tfOut 	= output + tfSufix;
		String idfOut 	= output + idfSufix;
		boolean status 	= false;
		long totalcsvdocs = 0;
		
//		Date currentHour = new Date();
//		String tempOutputFolder = "/tmp/" + DateTimeUtils.formatFullDateToFolderName(currentHour);
		
		try {
			WordCountDrive3 tf = new WordCountDrive3();
			status = tf.run(input, tfOut, numReducers);
			
			if (status) {
				totalcsvdocs = tf.totaldocs;
				
				IDFCalcDrive idf = new IDFCalcDrive();
				status = idf.run(tfOut, idfOut, totalcsvdocs, numReducers);
				if (status) {
					logger.info("Finished with success! Output: " + idfOut);
				}
				
			} else logger.error("Error running WordCountDrive MR.");
		} catch (Exception e) {
			logger.error("Ërror running MapReduceMainDriver. Error: " + e.getMessage());
			e.printStackTrace();
		}
	}
}
