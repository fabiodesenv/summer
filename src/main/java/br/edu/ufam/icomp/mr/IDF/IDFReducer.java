package br.edu.ufam.icomp.mr.IDF;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import br.edu.ufam.icomp.utils.Constants;

public class IDFReducer extends Reducer<Text, Text, Text, IntWritable> {
	private static Logger logger = Logger.getLogger(IDFReducer.class);
	private static final String TAB = Constants.TAB;
	private static final String COMMA = Constants.COMMA;
	private static final String PIPE = Constants.PIPE;
	private static long totaldocs = 0;

	@Override
	public void setup(Context context) throws IOException{
		Configuration conf = context.getConfiguration();
		totaldocs = conf.getLong("totaldocs", 0);
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException  {
		String term = key.toString();
		
		StringBuilder builder = new StringBuilder();
		int ndocs = 0;
		
		for (Text value : values) {
			try {
				String[] vals 	= value.toString().split(TAB);			
				String doc  	= vals[0];
				String freq	 	= vals[1];
				String ctid_header 	= vals[2];
				
				if (ndocs>0) builder.append(PIPE);				
			    builder.append(doc);
			    builder.append(COMMA);
			    builder.append(freq);
			    builder.append(COMMA);
			    builder.append(ctid_header);
			    ndocs++;
			    
				if ((ndocs == 10000000) || (ndocs == 20000000)) {
					logger.info("Counting term: " + key.toString());
					System.out.println("Counting term: " + key.toString());
				}
			} catch (Exception e) {
				logger.error("Error on reducer processing term " + term + ": " + e.getMessage());
				System.err.println("Error on reducer processing term " + term + ": " + e.getMessage());
				context.getCounter("FError", "ProcessingTerm").increment(1);
			}
		}
		
		if ( (ndocs > 20000000)) {
			logger.info("Counting term (>20000000): " + key.toString());
			System.out.println("Counting term (>20000000): " + key.toString());
		}
		
		double idf = Math.log10((double)totaldocs / (double)ndocs); //log na base 2 ?
		
		context.write(new Text(term + "," +idf + TAB + builder.toString()), new IntWritable(ndocs) );
	}
}