package br.edu.ufam.icomp.search;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import br.edu.ufam.icomp.utils.Constants;
import br.edu.ufam.icomp.utils.FileUtils;
import br.edu.ufam.icomp.utils.LoadHashKeyValue;
import br.edu.ufam.icomp.utils.StringHelper;
import br.edu.ufam.icomp.utils.TextAnalysisUtil;

public class Main {
	private static final String SEP = "=";
	private static final String FIELD_SEP = "#";
	private static Logger logger = Logger.getLogger(Main.class);
	private static int THRESHOLDTERMSET = 999999;

	public static void main(String[] args) throws IOException {
		if (args.length > 1 && args.length <= 4) {

			final File folder = new File(args[1]);

			if ( args[0].compareToIgnoreCase("-loadList") == 0 ) {
			} else {
				System.out.println("Error: wrong parameters. The valid parameters are: -loadList <path>");
				System.exit(-1);
			}
			
			Map<Long, String> mapIdToHeader = null;
			Map<Long, String> mapIdToTableName = null;

			if (folder.exists()) {

				Indexer indexer = null;

				try {					
					List<File> filelist = null;
					if (args[0].compareToIgnoreCase("-loadList") == 0)
						filelist = FileUtils.listFilesForFolder(folder, "noextension");
					
					if ((filelist != null) && (filelist.size() > 0)) {

						long timeTI1 = System.currentTimeMillis();						
						
						logger.info("Starting to load inverted index list .....");
						indexer = new Indexer(filelist, false);
						
						if (args.length >= 3) {
							long timemappers1 = System.currentTimeMillis();
							// loading the map file ID -> table name
							mapIdToTableName = LoadHashKeyValue.load(args[2]);
							
							// loading the map ID -> URL
							mapIdToHeader = LoadHashKeyValue.load(args[3]);
							long timemappers2 = System.currentTimeMillis();
							
							logger.info("Time to load the mappers: " + (timemappers2 - timemappers1) + " ms");
						}							
						
						
						long timeTI2 = System.currentTimeMillis();
						logger.info("Time to load inverted index: " + (timeTI2 - timeTI1) + " ms");
					} else 
						logger.error("Could not locate any file on folder: " + folder.getAbsolutePath());

				} catch (Exception e) {
					logger.error(e.getMessage());
					e.printStackTrace();
					System.exit(-10);
				}
 
				// log into a file the whole inverted index
				// indexer.saveindexFilesToFile("index.log");
				// System.exit(0);
				
				while (true) {
					try {
						InputStreamReader isr = new InputStreamReader(System.in);
						BufferedReader stdin = new BufferedReader(isr);
						System.out.println("Type \"exit\" to exit.");
						System.out.print("Input Keywords:");
						String input = stdin.readLine();

						if ((input == null) || (input.compareToIgnoreCase("exit") == 0))
							break;

						if (!StringHelper.isNullOrEmpty(input)) {
							ArrayList<String> Q = new ArrayList<String>(); // query
																			// set

							String[] tokens = TextAnalysisUtil.tokenize(input);

							// PRINT RESULT INFO - START
							System.out.println("\n--------QUERY-------------");
							StringBuilder query = new StringBuilder();
							query.append("Q={[");
							for (int i = 0; i < tokens.length; i++)
								if (i == 0)
									query.append(tokens[i]);
								else
									query.append(", " + tokens[i]);
							if (query.length() > 0)
								System.out.println(query.toString() + "]}");
							System.out.println("--------QUERY-------------\n");
							// PRINT RESULT INFO - START

							// PRINT RESULT INFO - START
							System.out.println("\n--------Inverted index-------------");
							for (String token : tokens) {
								List<String> results = indexer.getIndex(token.toLowerCase());
								if (results != null) {
									Q.add(token.toLowerCase());
									StringBuilder builderresult = new StringBuilder();
									int cont = results.size();
									for (String p : results) {
										String[] details = p.split(Constants.SEPATATOR);
										builderresult.append(details[3] + Constants.SEPATATOR + details[1] + Constants.SEPATATOR + "ctid=" + details[2]);
										if (cont > 1)
											builderresult.append(", ");
										cont--;
									}
									// System.out.println("{"+token + "} = {" +
									// builderresult.toString() + "}" + "\n");
								} else System.out.println("Term \"" + token + "\" did not match any document.");
							}
							System.out .println("--------Inverted index-------------\n");

							// PRINT RESULT INFO - START
							System.out .println("\n--------TupleSets creating ...-------------");
							List<List<String>> termsSets = new ArrayList<List<String>>();
							if (Q.size() > 0) {
								termsSets = indexer.createTermSets(Q, THRESHOLDTERMSET);
							}
							
							for (List<String> termSet : termsSets) {
								System.out.println(termSet);
							}
							
							System.out .println("\n-------- Query Matches Ranked------");
							SortedMap<Double, List<String>> QMRanked = new TreeMap<Double, List<String>>();
							QMRanked = indexer.CNRank(termsSets, Q);
							Double[] pesos = new Double[QMRanked.size()];
							int topk = 0;
							QMRanked.keySet().toArray(pesos);
							Arrays.sort(pesos, Collections.reverseOrder()); // This is what you mean

							for (Double peso : pesos) {
								// ++totalQM;
								if (topk < 10) {
									List<String> res = null;
									if ( (mapIdToHeader != null) && (mapIdToHeader.size() > 0) && (mapIdToTableName != null) && (mapIdToTableName.size() > 0))
										res = convertToNames(QMRanked.get(peso), mapIdToHeader, mapIdToTableName);
									else 
										res = QMRanked.get(peso);
									
									System.out.println("QM" + topk + ":" + res + "(" + peso + ")");
								}
								++topk;
							}

						}
					} catch (IOException ex) {
						ex.printStackTrace();
					}
				}
				System.out.println("Program exited successfuly!");
			} else  System.out.println("Folder " + args[0] + " does not exist.");
		} else {
			System.out.println("Usage: <java_program> <action> <path_folder>");
			System.out .println("where actions can be <loadList> or <createFromFiles>");
			System.out .println("Example: java -jar CSVParser -createFromFile /home/user/files");
			System.out .println("Example: java -jar CSVParser -loadList invertedlist.lst");
			System.exit(-2);
		}
	}
	
	private static List<String> convertToNames(List<String> qm, Map<Long, String> mapIdToHeader, Map<Long, String> mapIdToTableName) {
		List<String> result = null;
		
		if ((mapIdToHeader.size() > 0) && (mapIdToTableName.size() > 0)) {
			result = new ArrayList<String>();
			for (String s : qm) {
				Long tablenameID 	= null;
				Long headernameID = null;
				String keyword 	= null;
				String a[] = s.split(FIELD_SEP);
				if (a.length>=2) { // 8122975311571475497#malawi=washington
					tablenameID = Long.parseLong( a[0] );
					String[] b = a[1].split(SEP);
					headernameID = Long.parseLong( b[0] );
					if (b.length>1) {
						keyword = b[1];
						
						String tableID = tablenameID.toString();
						String headerID = headernameID.toString();
						
						if (mapIdToTableName.get(tablenameID)!=null) {
							tableID = mapIdToTableName.get(tablenameID);
							
							String[] hasUrl = tableID.split("\t");
							if (hasUrl.length>1)
								tableID=hasUrl[1] + FIELD_SEP + hasUrl[0];
						}
						
						if (mapIdToHeader.get(headernameID)!=null)
							headerID = mapIdToHeader.get(headernameID );
							
						result.add( tableID + FIELD_SEP + headerID + SEP + keyword);
					}						
				}
			}
			return result;
		}
		
		return result;
	}
}