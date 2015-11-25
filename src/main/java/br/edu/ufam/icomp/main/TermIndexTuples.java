package br.edu.ufam.icomp.main;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import br.edu.ufam.icomp.utils.Constants;

public class TermIndexTuples {
	private static Logger logger = Logger.getLogger(TermIndexTuples.class);
	public static HashMap<String, List<Item>> termIndexTuples = new HashMap<String, List<Item>>();
	
	public static void print(HashMap<String, HashMap<Integer, Integer>> termIndex, HashMap<Integer, String> map2attribute) {
		for (Map.Entry<String, List<Item>> entry : termIndexTuples.entrySet()) {
			String term = entry.getKey();
			StringBuilder builder = new StringBuilder();			
			builder.append(term+" -->\t");
			int counter = 0;
			for (Item i : entry.getValue()) {
				String table_column_name = i.getHeadercolumn();
				String tablename = i.getCsv().getName();
				if (counter>0)
					builder.append(", ");
				builder.append(tablename + "|" + table_column_name );
				counter++;
			}
			System.out.println(builder.toString());
		}
	}
	
	public static List<String> get( HashMap<String, HashMap<Integer, Integer>> termIndex, HashMap<Integer, String> map2attribute, String term ){
		List<Item> result = termIndexTuples.get(term);
		List<String> doclist = null;
		if (result == null)
			logger.error("Could not find any documents that match term " + term);
		else {
			doclist = new ArrayList<String>();
			for ( Item res : result ) {
				int attributeMapping = res.getAttributeMapping();
				String table_column_name = map2attribute.get(attributeMapping);
				String[] tablecolumnname = table_column_name.split(Constants.SEPATATOR);
				String tablename = tablecolumnname[0];
				String columnname = tablecolumnname[1];
				doclist.add(tablename + Constants.SEPATATOR + columnname
						+ Constants.SEPATATOR + res.getTupleid()
						+ Constants.SEPATATOR + res.getUri());
			}
		}
		return doclist;
	}

	public static List<Item> getTermSetPrunned(String term, HashMap<String, HashMap<Integer, Integer>> termIndex, Integer threshold, HashMap<String, Integer> attribute2map) {

		// pruning termsets with long-tail based on top-k threshold
		// pega toda lista invertida do termo
		List<Item> termsets = termIndexTuples.get(term);
		
		// se termo nao existe , nada faz
		if ( (termsets==null) || (termsets.isEmpty()) ) {
			logger.warn("No results for term: "+ term);
			return null;
		} else { return termsets; } 			
		
//		List<Item> result = new ArrayList<Item>();
//		int fkj = 0;
//		int Nj = 0;
//		int Na = 0;
//		int Ck = 0;
//		double wjk = 0.0;
//		SortedMap<Double, Item> termsetRanked = new TreeMap<Double, Item>();
//		
//		// para cada item da lista invertida
//		for (Item termset : termsets) {
//			// pega o id que identifica o nome da tabela + nome da coluna
//			int attributeMapping = termset.getAttributeMapping();
//			
//			// se sabemos o nome da tabela e coluna onde aparece
//			if (termIndex.get(term).get(attributeMapping) != null) {
//				fkj = termIndex.get(term).get(attributeMapping); // frequency fkj of term tk in values of attribute Bj
//				Nj = termIndex.size();
//				Na = attribute2map.size();
//				Ck = termIndex.get(term).size();
//				if (Nj > 0)
//					wjk += (Math.log(1 + fkj) / Math.log(1 + Nj))
//							* Math.log(1 + Na / Ck);
//				else
//					wjk = 0.0;
//			}
//			termsetRanked.put(wjk, termset);
//		}
//		
//		int topk = 1;
//		for (Item termsetR : termsetRanked.values()) {
//			if (topk < threshold) {
//				result.add(termsetR);
//			}
//			++topk;
//		}
//		return result;
	}
	
	public static void saveToFile( HashMap<String, HashMap<Integer, Integer>> termIndex, HashMap<Integer, String> map2attribute, String filename) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
		try {
			for (Map.Entry<String, List<Item>> entry : termIndexTuples .entrySet()) {
				String term = entry.getKey();
				StringBuilder builder = new StringBuilder();
				builder.append("\n" + term + " -->\t");
				int counter = 0;
				for (Item i : entry.getValue()) {
					String table_column_name = i.getHeadercolumn();
					String tablename = i.getCsv().getName();
					if (counter > 0)
						builder.append(", ");
					builder.append(tablename + "|" + table_column_name);
					counter++;
				}
				writer.write(builder.toString());
			}
		} catch (Exception e) {
			logger.error("Error whan saving index to a file. Error: " + e.getMessage());
		} finally {
			writer.close();
		}
	}
}
