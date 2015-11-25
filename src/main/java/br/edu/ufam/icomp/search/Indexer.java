package br.edu.ufam.icomp.search;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import br.edu.ufam.icomp.main.Item;
import br.edu.ufam.icomp.main.TermIndexTuples;
import br.edu.ufam.icomp.main.objectdomain.CtidHeader;
import br.edu.ufam.icomp.main.objectdomain.Element;
import br.edu.ufam.icomp.main.objectdomain.ParseInvertedIndexList;
import br.edu.ufam.icomp.parsers.csv.CSVParser;
import br.edu.ufam.icomp.utils.Constants;

public class Indexer {
	private static Logger logger = Logger.getLogger(Indexer.class);
	private HashMap<String, HashSet<String>> index;
	
	private static HashMap<String, HashMap<Integer, Integer>> termIndex  = new HashMap<String, HashMap<Integer, Integer>>();
	private static HashMap<String, Integer> attribute2map = new HashMap<String, Integer>();
	private static HashMap<Integer, String> map2attribute = new HashMap<Integer, String>();
	
	
	public Indexer(){
		index = new HashMap<String, HashSet<String>>();
	}
	
	public Indexer(List<File> filelist, Boolean createList) throws IOException {
		index = new HashMap<String, HashSet<String>>();
		indexFiles(filelist, createList);
	}
	
	private void indexFiles(List<File> filelist, Boolean createList) throws IOException {
		long cont = 0;
		
		long timeBegin = System.currentTimeMillis();		
		for (File f : filelist) {
			cont++;
			if ((cont % 1000) == 0) // only for progress purpose
				logger.info("Indexing " + cont + " of " + filelist.size() + " (" + Double.toString((double) cont / filelist.size()) + "%)");
			
			// load JSON files
			if (f.getName().endsWith(Constants.JSON_EXT) || (createList == false)) {

				if (createList) { // create list from file
					List<Item> items = null;
					items = CSVParser.parser(f, filelist);

					System.out .println("\n\nInserting inverted list into specific structures. Total itens: "
									+ items.size());
					long count = 0;
					for (Item i : items) {
						
						if (count % 100000 == 0) {
							System.out.print(count * 100 / items.size() + "% ");
						}

						//String table = i.getFilename();
						String table = i.getCsv().getName();
						// String table = i.getUri().toString();
						add(i.getWord(), table);

						// attribute2map <nome_arquivo.nomedacoluna, incrimental_id>
						Integer attributeMapping = attribute2map.get(table + Constants.SEPATATOR + i.getHeadercolumn());
						if (attributeMapping == null) {
							attribute2map.put( table + Constants.SEPATATOR + i.getHeadercolumn(), attribute2map.size() + 1);
							map2attribute .put(map2attribute.size() + 1, table + Constants.SEPATATOR + i.getHeadercolumn());
							attributeMapping = 1;
						}

						i.setAttributeMapping(attributeMapping);
						int frequency = 0;
						// termIndex <word, <attributeMapping, constant_1>>
						if (termIndex.containsKey(i.getWord())) {
							termIndex.get(i.getWord()).put(attributeMapping, 1);
							// updated by Pericles
							if (termIndex.get(i.getWord()).get(attributeMapping) != null)
								frequency = termIndex.get(i.getWord()).get(attributeMapping);
							if (frequency > 0)
								termIndex.get(i.getWord()).put(attributeMapping, frequency + 1);
							else
								termIndex.get(i.getWord()).put(attributeMapping, 1);
							// end updated

						} else {
							termIndex.put(i.getWord(), new HashMap<Integer, Integer>());
							termIndex.get(i.getWord()).put(attributeMapping, 1);
						}

						// HashMap<String, HashMap<String, Integer>> -->
						// TermIndexTuples<word, HashMap<ctid,
						// attributemapping>>
						if (TermIndexTuples.termIndexTuples.containsKey(i.getWord())) {
							TermIndexTuples.termIndexTuples.get(i.getWord()).add(i);
						} else {
							TermIndexTuples.termIndexTuples.put(i.getWord(),new ArrayList<Item>());
							TermIndexTuples.termIndexTuples.get(i.getWord()).add(i);
						}
						count++;
					}
				}
				// load a list already created
				else { // only load an already created list
					long elementCount = 0;
					List<ParseInvertedIndexList> invertedlist = loadInvertedList(f);
					System.out.println("\n\n\n\n------------------- Inserting file " + f + " into structures.");
					for (ParseInvertedIndexList parse : invertedlist) {
						for (Element e : parse.getElement()) {
							for (CtidHeader c : e.getCtidH()) {
								
								String table = e.getCsvname();
								
								Item i = new Item(parse.getWord(), c.getColumnsname(), -1, table, c.getCtid());
								i.setFilename(e.getCsvname());
								
								elementCount++;
								
								if (elementCount % 100000 == 0) {
									System.out.println("Elements processed/Total: " + elementCount + "/" + parse.getTotalelements() + " (" + elementCount * 100 / parse.getTotalelements() + "%)");
								}

								indexFilesFromItem(i);
							}
						}				
					}
				}
				//System.out.println("");
			}
		}
		long timeEnd = System.currentTimeMillis();
		System.out.println("Finished. Loading all files took " + (timeEnd - timeBegin) + " ms");
		System.out.println();
	}
	
	public void indexFilesFromItem(Item i) {
		String table = i.getFilename();
		
		add(i.getWord(), table); // TO-DO pra que serve este estrutura? pa nada?

		// attribute2map <nome_arquivo.nomedacoluna, incrimental_id>
		Integer attributeMapping = attribute2map.get(table + Constants.SEPATATOR + i.getHeadercolumn());
		if (attributeMapping == null) {
			attribute2map.put( table + Constants.SEPATATOR + i.getHeadercolumn(), attribute2map.size() + 1);
			map2attribute .put(map2attribute.size() + 1, table + Constants.SEPATATOR + i.getHeadercolumn());
			attributeMapping = 1;
		}

		i.setAttributeMapping(attributeMapping);
		int frequency = 0;
		// termIndex <word, <attributeMapping, constant_1>>
		if (termIndex.containsKey(i.getWord())) {
			termIndex.get(i.getWord()).put(attributeMapping, 1);
			// updated by Pericles
			if (termIndex.get(i.getWord()).get(attributeMapping) != null)
				frequency = termIndex.get(i.getWord()).get(attributeMapping);
			if (frequency > 0)
				termIndex.get(i.getWord()).put(attributeMapping, frequency + 1);
			else
				termIndex.get(i.getWord()).put(attributeMapping, 1);
			// end updated

		} else {
			termIndex.put(i.getWord(), new HashMap<Integer, Integer>());
			termIndex.get(i.getWord()).put(attributeMapping, 1);
		}

		// HashMap<String, HashMap<String, Integer>> -->
		// TermIndexTuples<word, HashMap<ctid,
		// attributemapping>>
		if (TermIndexTuples.termIndexTuples.containsKey(i.getWord())) {
			TermIndexTuples.termIndexTuples.get(i.getWord()).add(i);
		} else {
			TermIndexTuples.termIndexTuples.put(i.getWord(),new ArrayList<Item>());
			TermIndexTuples.termIndexTuples.get(i.getWord()).add(i);
		}
	}
	
	private void add(String word, String fileName) {
		if (index.containsKey(word)) {
			if (!index.get(word).contains(fileName))
				index.get(word).add(fileName);
		} else {
			HashSet<String> doc = new HashSet<String>();
			doc.add(fileName);
			index.put(word, doc);
		}
	}
	
	@SuppressWarnings("rawtypes")
	public void printIndex() {		
		Iterator it = index.entrySet().iterator();
		while (it.hasNext()) {
			
			Map.Entry pair = (Map.Entry)it.next();
	       System.out.print(pair.getKey() + " = ");
	       logger.debug(pair.getKey() + " = ");
	       @SuppressWarnings("unchecked")
			HashSet<String> v = (HashSet<String>) pair.getValue();
	       for (String s : v) {
	           System.out.print(" --> " + s);
	           logger.debug(" --> " + s);
	       }
	       System.out.println("");
	       //it.remove(); // avoids a ConcurrentModificationException
		}
	}
	
	public HashMap<String, HashSet<String>> getIndex() {
		return index;
	}

	public List<String> getRawIndex(String word) {
		List<String> doclist = null;
		if (index.containsKey(word)) {
			HashSet<String> docs = index.get(word);
			if (docs.size()>0) {
				doclist = new ArrayList<String>();
				for (String doc : docs)
					doclist.add(doc);
			} else logger.error("The term " + word + " has no occurence in any document.");
			
		}
		return doclist;
	}
	
	public List<String> getIndex(String term) {
		return TermIndexTuples.get(termIndex, map2attribute, term);
	}
	
	public void saveindexFilesToFile(String filename) {
		try {
			TermIndexTuples.saveToFile(termIndex, map2attribute, filename);
		} catch (IOException e) {
			logger.error("Could not save the index files: saveindexFilesToFile. Error: " + e.getMessage()); 
			e.printStackTrace();
		}
	} 
	
	public List<List<String>> createTermSets(List<String> query,int thresholdTail) {
		HashMap<String, ArrayList<Integer>> tupleSets = new HashMap<String, ArrayList<Integer>>();
		HashMap<String, ArrayList<String>> tupleSetsTerm = new HashMap<String, ArrayList<String>>();

		int maxKeywordJoin = 0;

		int p;
		
		// pega toda lista invertida para cada termo da consulta e guarda em tupleSets e tupleSetsTerm ("desinverte")
		// for each term on query
		
		for (String keyword : query) {
    		// get all inverted index for this term (pruned by a limit) max=1589008
			logger.debug("Calling getTermSetPrunned");
			long time1 = System.currentTimeMillis();
			List<Item> it = TermIndexTuples.getTermSetPrunned(keyword,termIndex,thresholdTail,attribute2map);
			long time2 = System.currentTimeMillis();
			// System.out.println("\ngetPRunned took " + (time2-time1) + " ms");
			if ( (it != null) && (!it.isEmpty()) ) {
				int count = 0;
				for (Item item : it ) {					
					count++;
					if (count%1000==0) {
						System.out.println( count + "/" + it.size() + " - " + keyword);
					}
					
					String tupleID = item.getTupleid();			
					if (tupleSets.get(tupleID) == null) // stores all different tupleid where term occur 
						tupleSets.put(tupleID, new ArrayList<Integer>()); // insere o novo ctid.
					// Ex.: CTID, [attribute2map1,attribute2map2,attribute2map3...] onde attributeid é o ID do nome da tabela + nome da coluna
					
					if (tupleSetsTerm.get(tupleID) == null)
						tupleSetsTerm.put(tupleID, new ArrayList<String>()); // insere o novo ctid
					// Ex.: CTID, [nome da tabela + nome da coluna + "=" + keyword, ...]
					
					tupleSets.get(tupleID).add(item.getAttributeMapping()); // adiciona na lista do citd, nome da tabela + nome da coluna
					tupleSetsTerm.get(tupleID).add(map2attribute.get(item.getAttributeMapping())+ "=" + keyword );
				}
				//System.out.println();
			}
		}
		
		// junta todas as ocorrencias de todas as palavras da consula em um único termSets
		HashMap<String, ArrayList<String>> termSets = new HashMap<String, ArrayList<String>>();

		// for each different tupleid where any of the terms on query occur
		for (String tupleID : tupleSets.keySet()) {
			if (tupleSets.get(tupleID).size() > 0) {

				String attributeBefore = new String();
				String attributeCurrent = new String();
				String keywordJoin = new String();
				String keywordJoinAttribute = new String();
				int startCounter = 0;
				
				// all these attribute+keyword are in same tuple termSets.put(attributeTerm,0);
				// pega todas as tabelas, colunas e termos que ocorrem neste tupleid (lista desinvertida) 
				for (String attributeTerm : tupleSetsTerm.get(tupleID)) { // attributeTerm = table#column#word
					p = attributeTerm.indexOf('=');
					// attributeTerm example: http://www.bestbuyic.com/BestbuyIC/html14/MG100J6ES45.html#"*"=brazil
					
					if (p >= 0) {
						attributeCurrent = attributeTerm.substring(0, p); // http://www.site.com/oi.html#"column"
					}
					
					// termSets (termo, new ArrayList<String>()). Se nao existe chave para o termo eu crio
					if (termSets.get(attributeTerm.substring(p + 1, attributeTerm.length())) == null)
						termSets.put(attributeTerm.substring(p + 1, attributeTerm.length()), new ArrayList<String>());
					
					// termSets (termo, table#column=word). Se o elemento para este termo ainda nao existe, entao adiciono
					if (!(termSets.get(attributeTerm.substring(p + 1, attributeTerm.length())).contains(attributeTerm)))
						termSets.get(attributeTerm.substring(p + 1, attributeTerm.length())).add(attributeTerm);
					
					if (startCounter == 0) // controle pra saber se mudou ou nao. attributeCurrent = "nomedatabela + nomedacoluna"
						attributeBefore = attributeCurrent;
					
					if (attributeCurrent.equals(attributeBefore)) {
						if (startCounter == 0) {
							keywordJoinAttribute += attributeCurrent + "=" + attributeTerm.substring(p + 1, attributeTerm.length());
							keywordJoin += attributeTerm.substring(p + 1, attributeTerm.length());
						} else {
							keywordJoinAttribute += "_" + attributeTerm.substring(p + 1, attributeTerm.length());
							keywordJoin += "_" + attributeTerm.substring(p + 1, attributeTerm.length());
						}
						++startCounter;
						// example query test_test
						// keywordJoinAttribute = http://www.long-lake-mn-real-estate.com/test/php/test.php#"value"=test_test
						// keywordJoin = test_test
					}
					attributeBefore = attributeCurrent;
				}
				
				if (startCounter > maxKeywordJoin) {
					maxKeywordJoin = startCounter;
					//maxKeyword = keywordJoin;
				}
				// adicionando novas chaves aos termsets já existes. COmposição de palavras que ocorrem na mesma tupleid
				if (termSets.get(keywordJoin) == null)
					termSets.put(keywordJoin, new ArrayList<String>());
				if (!(termSets.get(keywordJoin).contains(keywordJoinAttribute)))
					termSets.get(keywordJoin).add(keywordJoinAttribute);
			}
		}
		
		// add empty sets
		//int currentSize;
		for (String term : termSets.keySet()) {
			termSets.get(term).add("{}");
		}
		
		// create the cartesian product
		List<List<String>> matrix = new ArrayList<List<String>>();
		for(String term:termSets.keySet()) 
			matrix.add(new ArrayList(termSets.get(term)));
		
		return genDistributions(matrix);
	}
	
	
    public static List<List<String>> removeDuplicate(List<List<String>> termSets) {
	
	for (int i = 1; i < termSets.size(); i++) {
            List<String> a1 = termSets.get(i);
            List<String> a2 = termSets.get(i-1);
            if (a1.equals(a2)) {
                termSets.remove(a1);
            }
        }
	return termSets;
    }
    public static List<List<String>> genDistributions(List<List<String>> lists) {
      List<List<String>> resultLists = new ArrayList<List<String>>();
      List<String>    terms          = new ArrayList<String>();
      boolean validCovering=true;
      if (lists.size() == 0) {
        resultLists.add(new ArrayList<String>());
        return resultLists;
      } else {
        List<String> firstList = lists.get(0);
        List<List<String>> remainingLists = genDistributions(lists.subList(1, lists.size()));
        validCovering=true;
        for (String condition : firstList) {
          //System.out.println("condition:"+condition+" firstList:"+firstList);
          for (List<String> remainingList : remainingLists) {
            terms.clear();
            validCovering=true;
            if(condition!="{}") {
              String[] termSet        = condition.split("="); //e.g: name.name
              String[] termSetPart    = termSet[1].split("_");//e.g: denzel_washington
              String[] tablePart      = termSet[0].split("\\.");//e.g: name
              for(String termPart: termSetPart) {
                  if(terms.contains(termPart)) validCovering=false;
                  else terms.add(termPart);
              }
            }
            for(String condition2 : remainingList) {
               if(condition2!="{}") {
                String[] termSet2        = condition2.split("="); //e.g: name.name
                String[] termSetPart2    = termSet2[1].split("_");//e.g: denzel_washington
                String[] tablePart2      = termSet2[0].split("\\.");//e.g: name
                for(String termPart2: termSetPart2) {
                   if(terms.contains(termPart2)) validCovering=false;
                   else terms.add(termPart2);
                }
              }
            }
            //System.out.println("remainList:"+remainingList+" RLists:"+remainingLists);
            ArrayList<String> resultList = new ArrayList<String>();
            if(validCovering) {
                resultList.add(condition);
                resultList.addAll(remainingList);
                resultLists.add(resultList);
            }
          }
        }
      }
      return resultLists;
    }
   public SortedMap<Double,List<String>> CNRank(List<List<String>> termSets,List<String> query) {
	     SortedMap<Double,List<String>> QMRanked          = new TreeMap<Double, List<String>>();
		 Integer fkj    = 0;
         double  wjk    = 0.0; // weights according to the TF-IAF model
         double  accwjk = 0.0;
         double  scorei = 0.0;
         Integer Nj     = 0; // total number of distinct terms that occur in values of the attribute Bj
         Integer Na     = 0; // total number of attributes in the database
         Integer Ck     = 0; // number of attributes in whose values the term k occur
         /*
                wjk = log(1+fkj)/log(1+Nj) x log(1+Na/Ck)
         */
         // check covering conditions for each match query
         QMRanked.clear();
         for(List<String> match: termSets)
         {
            List<String> matchValid     = new ArrayList<String>();
            List<String> terms          = new ArrayList<String>();
            HashMap<String,Integer> RLR = new HashMap<String,Integer>();// conditon R->L->R
            Integer sizeMatch           = 0;
            boolean validCovering       = true;
            accwjk                      = 0.0;
            scorei                      = 0.0;
            RLR.clear();
            for(String matchPart: match)
            {   wjk =0.0;
                if(matchPart!="{}") {
                        ++sizeMatch;
                        matchValid.add(matchPart); //something like that name.name=denzel
                        //check if match is total
                        String[] termSet        = matchPart.split("="); //e.g: name.name
                        String[] termSetPart    = termSet[1].split("_");//e.g: denzel_washington
                        String[] tablePart      = termSet[0].split("\\#");//e.g: name
                        if(tablePart.length>1) {
                                if(RLR.get(tablePart[0])==null) RLR.put(tablePart[0],1);
                                else RLR.put(tablePart[0],RLR.get(tablePart[0])+1);
                        }
                        int contSameTuple       = 0;
                        for(String termPart: termSetPart) {
                                fkj     =0;
                                Nj      =0;
                                Na      =0;
                                if(terms.contains(termPart)) validCovering=false;
                                else terms.add(termPart);
				if(termSet[0]!=null) {
                                  if(termIndex.get(termPart).get(attribute2map.get(termSet[0]))!=null) {
                                    fkj = termIndex.get(termSetPart[0]).get(attribute2map.get(termSet[0])); //frequency fkj of term tk in values of attribute Bj
                                    Nj  = termIndex.size();
                                    //Nj  = distinctAttrbutes.get(attribute2map.get(termSet[0])).size();
                                    Na  = attribute2map.size();
                                    Ck  = termIndex.get(termPart).size();
                                    if(Nj>0) wjk += (Math.log(1+fkj)/Math.log(1+Nj)) * Math.log(1+Na/Ck);
                                    else wjk=0.0;
                                    //wjk          += Math.log(1+fkj) / Math.log(1+Nj);
                                    //wjk += contSameTuple;
                                    contSameTuple +=2;
                                  }
				}
                        }
                        if(accwjk==0.0) accwjk = wjk;
                        else accwjk *= wjk;
                }
                scorei = accwjk * 1/match.size();
            }
            // check RLR condition
            for(Integer RLRcount:RLR.values())
            {
                if(RLRcount==sizeMatch && RLRcount>1) validCovering=false;
            }
            //check if match is minimal
            for(String keyword:query) {
                if(!terms.contains(keyword)) validCovering=false;
            }
            if(validCovering) QMRanked.put(scorei,matchValid);
            terms.clear();
          }
		return QMRanked;
   }
   
   private List<ParseInvertedIndexList> loadInvertedList(File path) throws IOException {
	   List<ParseInvertedIndexList> lines = new ArrayList<>();
		FileInputStream inStream = null;
		DataInputStream inDataStream = null;
		BufferedReader br = null;
		long timeWholeProcess = System.currentTimeMillis();

		System.out.println("\n-------------------------------------");
		System.out.println("Processing: " + path.toString());
		
		try {
			inStream = new FileInputStream(path);
		
			inDataStream = new DataInputStream(inStream);
			br = new BufferedReader(new InputStreamReader( inDataStream ));
	
			String str = null;
			long counter = 0;
			long time = System.currentTimeMillis();
			while ((str = br.readLine()) != null) {
				long timeByLine = System.currentTimeMillis();
				ParseInvertedIndexList parser  = new ParseInvertedIndexList(str, false);
		        lines.add(parser);
		        System.out.println("----------------Term " + parser.getWord() + " with total docs " +
		                            parser.getTotalelements() + " has elements: " + parser.getElement().size());
		        
				counter++;
				long timeByLine2 = System.currentTimeMillis();
//				if (counter%1==0) {
//		        	System.out.println("Read " + counter + " lines.");
//		        	System.out.println("The last line took: "+ (timeByLine2 - timeByLine) + " ms. Number of elements " + parser.getTotalelements());
//		        }
			}
			long time2 = System.currentTimeMillis();
			System.out.println("Finished. ParseInvertedIndexList took " + (time2 - time) + " ms");
			
			System.out.println("Finished. Read " + counter + " lines.");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			inStream.close();
			inDataStream.close();
			br.close();
		}
		long timeWholeProcess2 = System.currentTimeMillis();
		System.out.println("Finished. The whole process took " + (timeWholeProcess2 - timeWholeProcess) + " ms");
		
		return lines;
	}		        
   
}
	
