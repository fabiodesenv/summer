package br.edu.ufam.icomp.parsers.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import br.edu.ufam.icomp.main.Item;
import br.edu.ufam.icomp.utils.Constants;
import br.edu.ufam.icomp.utils.FileUtils;
import br.edu.ufam.icomp.utils.StringHelper;
import br.edu.ufam.icomp.utils.TextAnalysisUtil;

public class CSVParser {
	private static Logger logger = Logger.getLogger(CSVParser.class);

	public static List<Item> parser(File file, List<File> filelist) {
		List<Item> words 	= new ArrayList<Item>();
		BufferedReader br 	= null;
		String filecontent 	= null;
		
		try {
			
			filecontent = FileUtils.readSmallFiles(file);
			JSONObject metadata = new JSONObject(filecontent);
			//String filename = metadata.getString("filename");
			URI uri = new URI(metadata.getString("uri"));
			
			File filecsv = FileUtils.getCSVFromJSON(file, filelist);
			if (filecsv != null) {
				file = filecsv;
				br = new BufferedReader(new FileReader(file));
				String line;
				// lendo linha a linha porque vou precisar marcar a linha em que ocorre depois
				line = br.readLine();
				String header_columns[] = line.split(Constants.COMMA);
				int iline=1;
				while ((line = br.readLine()) != null) { // loop by line
					iline++;
					// loop by collumns
					String columns[] = line.split(Constants.COMMA);
					for (int j=0;j<columns.length;j++) {
						//loop by word
						String titleTokens[] = TextAnalysisUtil.tokenize(columns[j]);
						for (int i=0;i<titleTokens.length;i++) {
							if (!StringHelper.isNullOrEmpty(titleTokens[i])
									&& !StringHelper.isNumeric(titleTokens[i])
									&& titleTokens[i].length() >= Constants.MINIMUM_WORD_SIZE
									&& titleTokens[i].length() <= Constants.MAXIMUM_WORD_SIZE
									&& header_columns.length > j && !StringHelper.isNullOrEmpty(header_columns[j]) // TODO existem muitas colunas com o nome NULL, devo remover?
									&& header_columns[j].length() <= Constants.MAXIMUM_HEADER_SIZE) {
								String ctid = Long.toString(StringHelper.generateMd5(file.getName() + iline + header_columns[j]));
								Item item = new Item(titleTokens[i].toLowerCase(), header_columns[j], iline, uri, file, ctid);
								words.add(item);
							} else logger.debug("Discarding: " + titleTokens.toString() + " | " + iline + " | " + header_columns.toString() );
						}
					}
				}
			} else logger.error("Not able to find csv related fo " + file.getName());
		} catch (FileNotFoundException e) {
			logger.error("Error FileNotFoundException CSV file. Error: " + e.getMessage() + " File: " + file.getAbsolutePath());
			//e.printStackTrace();
		} catch (IOException e) {
			logger.error("Error IOException CSV file. Error: " + e.getMessage() + " File: " + file.getAbsolutePath());
			//e.printStackTrace();
		} catch (JSONException e) {
			logger.error("Error reading json file. Error: " + e.getMessage() + ". File: " + file.getAbsolutePath() + " JSON: " + filecontent);
		} catch (Exception ex) {
			logger.error("Error parsing CSV file. Error: " + ex.getMessage() + " File: " + file.getAbsolutePath());
			ex.printStackTrace();
		} 
		finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}		
		
		return words;
	}
}
