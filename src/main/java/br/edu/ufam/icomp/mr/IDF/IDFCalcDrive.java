package br.edu.ufam.icomp.mr.IDF;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IDFCalcDrive {
	public static void main(String[] args) throws Exception {
		if ( (args.length < 3) || (args.length > 4) ){
			System.out.printf("Usage: WordCount <input dir> <output dir> <total_docs> [optional:number_reducers]\n");
			System.exit(-1);
		}
		int numReducers = 0;
		if (args.length == 4) {
			numReducers = Integer.parseInt(args[3]);
		}
				
		IDFCalcDrive idfcalc = new IDFCalcDrive();
		idfcalc.run(args[0], args[1], Long.parseLong(args[2]), numReducers);
	}
	
	public boolean run(String input, String output, long totaldocs, int numReducers) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		conf.setLong("totaldocs", totaldocs);
		Job job = Job.getInstance(conf, "IDF - MapReduce");		
		
		job.setJarByClass(IDFCalcDrive.class);
		job.setJobName("IDF - MapReduce");
		
		if (numReducers > 0)
			job.setNumReduceTasks(numReducers);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(IDFMapper.class);
		job.setReducerClass(IDFReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		boolean success = job.waitForCompletion(true);
		
		return (success ? true : false);
	}
}
