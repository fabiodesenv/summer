package br.edu.ufam.icomp.mr.WordCount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import br.edu.ufam.icomp.mr.FileInputFormat.WholeFileInputFormat3;
import br.edu.ufam.icomp.utils.HDFSUtil;
import br.edu.ufam.icomp.utils.TextAnalysisUtil;

public class WordCountDrive3 {
	private static Logger logger = Logger.getLogger(WordCountDrive3.class);
	public long totaldocs = 0;
	
	public static void main(String[] args) throws Exception {
		if ( (args.length < 2) || (args.length > 3)) {
			System.out.printf("Usage: WordCount <input dir> <output dir> [true/false]\n");
			System.out.printf("true means yes for multiple path/ false means no for multiple path]\n");
			System.exit(-1);
		}
		
		Boolean multiplePath = true;
		
		if ( (args.length == 3) && (args[2].compareToIgnoreCase("false")==0) ) {
			multiplePath = false;
			System.out.println("Using single path");
		} else System.out.println("Using multiple path");

		WordCountDrive3 invertedindex = new WordCountDrive3();
		invertedindex.run(args[0], args[1], multiplePath, 0);
	}
	
	public boolean run(String input, String output, int numReducers) throws IOException, ClassNotFoundException, InterruptedException {
		return run(input, output, true, numReducers);
	}
	
	public boolean run(String input, String output, Boolean multiplePath, int numReducers) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TF - WordCount MapReduce 2");
		
		job.setJarByClass(WordCountDrive3.class);
		job.setJobName("TF - WordCount MapReduce 2");

		//FileInputFormat.setInputPaths(job, new Path(input));
		System.out.println("Processing multiple input");
		List<String> inputs = new ArrayList<String>();
		
		// if we are going to use multiple paths as input
		if (multiplePath) {
			inputs = generateInputs(input); // build multiple paths as input
			if ( (inputs == null) || (inputs.size() ==0)) {
				System.out.println("Could not build the multiple input paths");
				logger.error("Could not build the multiple input paths");
				return false;
			}
		} else { // only one path or file as input
			inputs.add(input);
		}
			
		for (String path : inputs){			
			FileInputFormat.addInputPath(job, new Path(path));			
		}
		
		System.out.println("Done multiple input. Number of input folders: " + inputs.size());		
		
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setInputFormatClass(WholeFileInputFormat3.class);
		
		if (numReducers > 0)
			job.setNumReduceTasks(numReducers);
		
		// initialize the stop words
		System.out.println("Initializing stop words");
		TextAnalysisUtil.startStopwords();
		
		job.setMapperClass(WordCountMapper3.class);
		job.setReducerClass(WordCountReducer3.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
			
		boolean success = job.waitForCompletion(true);
		if (!success) {
			logger.error("Error with CountRequestsMRConfig.");
			throw new IOException("error with job!");
		} else {
			Counters counters = job.getCounters();
			
			String name = counters.findCounter("FCounter", "CSVProcessed").getDisplayName();
			totaldocs = counters.findCounter("FCounter", "CSVProcessed").getValue();
			logger.info("RESULTADO: " + name + ": " + " : " + totaldocs);
		}
		
		return (success ? true : false);
		
	}	
	
	private static List<String> generateInputs (String inputpath) {
		List<String> inputPaths = new ArrayList<String>();
		
		try {
			FileStatus[] fstats = HDFSUtil.listStatus(inputpath);
			
			if (fstats == null) {
				logger.error("Could not read folders from: " + inputpath);
				System.out.println("Could not read folders from: " + inputpath);
			} else {
			
				for (FileStatus fstat : fstats) {
					inputPaths.add(fstat.getPath().toString());
	            }		
			}	
				
		} catch (IOException e) {
			logger.error("ERROR generating input paths. " + e.getMessage());
		}	
		
		return inputPaths;
	}

}
