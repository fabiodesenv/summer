package br.edu.ufam.icomp.mr.IDF;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONArray;
import org.json.JSONObject;

import br.edu.ufam.icomp.utils.TextAnalysisUtil;

public class IDFMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final int MAX_OCURRENCE_WITHIN_DOC = 30;
	private static final int MAX_STRING_SIZE = 50;
	//private static Logger logger = Logger.getLogger(IDFMapper.class);
	private static final String TAB = "\t";
	
	@Override
	public void setup(Context context) throws IOException{
		TextAnalysisUtil.startStopwords();
	}

	@Override
	public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {

		String line[] = value.toString().split(TAB);
		String term = line[0];
		String doc = line[1];
		String jsonstr = line[2];
		long freq = Long.valueOf(line[3]);
		
		// limit number of occurences on same doc for MAX_OCURRENCE_WITHIN_DOC times
		JSONArray jarray = new JSONArray(jsonstr);
		
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < jarray.length(); i++) {
			try {
				JSONObject json = jarray.getJSONObject(i);
				if (i>0) builder.append("-");
				builder.append(json.getString("id")); // tupleid
				builder.append(",");
				builder.append(truncate(json.getString("hc"))); // header da coluna
			} catch (Exception e) {System.err.println(e.getMessage());}
			if (i >= MAX_OCURRENCE_WITHIN_DOC) break;
		}
		
		if ( TextAnalysisUtil.isValidTerm(term) )
			context.write(new Text(term) , new Text(doc + TAB + freq + TAB + builder.toString()));
		else context.getCounter("FCounter", "InvalidTerm").increment(1);
		
	}
	
	private static String truncate(String input) {
		if (input.length() > MAX_STRING_SIZE) {
			return input.substring(0, MAX_STRING_SIZE);			
		} else 
			return input;
	}

}