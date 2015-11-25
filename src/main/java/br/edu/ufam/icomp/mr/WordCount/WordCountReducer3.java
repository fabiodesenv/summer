package br.edu.ufam.icomp.mr.WordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class WordCountReducer3 extends Reducer<Text, Text, Text, IntWritable> {
	private static Logger logger = Logger.getLogger(WordCountReducer3.class);
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int wordcount = 0;
		JSONArray jsonarray = new JSONArray();		

		for (Text value : values) {
			wordcount++;
			try {
				JSONObject json = new JSONObject(value.toString());
				jsonarray.put(json);				
			} catch (JSONException e) {
				logger.error("Error trying to create JSON object on WordCountReducer. Error: " + value.toString());
				e.printStackTrace();
			}			
		}
		
		context.write(new Text(key + "\t" + jsonarray.toString()), new IntWritable(wordcount));
	}
}
