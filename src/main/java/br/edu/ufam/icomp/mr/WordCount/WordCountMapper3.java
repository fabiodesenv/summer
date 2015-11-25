package br.edu.ufam.icomp.mr.WordCount;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.security.InvalidParameterException;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import br.edu.ufam.icomp.main.Item;
import br.edu.ufam.icomp.utils.Constants;
import br.edu.ufam.icomp.utils.StringHelper;
import br.edu.ufam.icomp.utils.TextAnalysisUtil;

public class WordCountMapper3 extends Mapper<Text, BytesWritable, Text, Text> {
	private static Logger logger = Logger.getLogger(WordCountMapper3.class);

	public void map(Text key, BytesWritable value,Context context) throws java.io.IOException ,InterruptedException
	{
		String gzFileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		//System.out.println("Tar file " + filename);
		gzFileName = context.getCurrentKey().toString(); 
		Configuration conf = context.getConfiguration();
		
		//logger.info("Processing file " + filename);
		System.out.println("Processing file within tar: " + gzFileName);
		
		//byte[] fileInBytes = new byte[(int)value.getSize()];
		//fileInBytes = value.get();
		//byte[] fileInBytes = value.copyBytes();
		InputStream gzIn = new ByteArrayInputStream(value.copyBytes());
		InputStream tarIn = null;
		TarArchiveInputStream tarArchiveInputStream = null;
		
		try {			
			CompressionCodecFactory factory = new CompressionCodecFactory(conf);
			// get compress coded (gzip)
			CompressionCodec codec = factory.getCodec(new Path(gzFileName));
		
			// reading .gz file, get compress coded (gzip)
			tarIn = codec.createInputStream(gzIn);
			
			tarArchiveInputStream = new TarArchiveInputStream( tarIn );
			
			TarArchiveEntry entry = null;
			byte[] buffer = new byte[130560]; // 127,5 Kb
			
			while ((entry = tarArchiveInputStream.getNextTarEntry()) != null) {
				try {
					context.getCounter("FCounter", "NumberOfTotalInputFiles").increment(1);
					
					if (tarArchiveInputStream.canReadEntryData(entry)) {
						if ( entry.getName().endsWith(Constants.CSV_EXT) ) {
							
							if ( entry.getSize() < 209715200) { // do not process files larger then 200MB
								
								String filename = entry.getName(); 
								
								logger.info("Processing file " + filename + " size: " + entry.getSize());
								System.out.println("Processing file " + filename + " size: " + entry.getSize());							
								
								context.getCounter("FCounter", "CSVProcessed").increment(1);
								
								int n = 0;
								ByteArrayOutputStream os = new ByteArrayOutputStream();
								while (-1 != (n=tarArchiveInputStream.read(buffer))) {
							          os.write(buffer, 0, n);
							    }
			
								String alllines = os.toString("UTF-8");
								os.close();
			
							    String[] eachline = alllines.split("\n"); // break into lines
							    
							    String[] columnsnames = eachline[0].split(Constants.COMMA); // get columns names
							    logger.debug("File name, number of lines, number of columns: " + filename + ", " + eachline.length + ", " + columnsnames.length);
							    
							    // loop by line, starting from line 2 (cause line 1 are headers)
							    for (int line=1;line<eachline.length;line++) {
							    	
							    	// loop by columns
							    	String[] columns = eachline[line].split(Constants.COMMA); // split into columns values
							    	for (int column=0;column<columns.length;column++) { // for each column (field)
							    		
							    		if (column < columnsnames.length) { // if the number of columns does not match the number of headers, then no reason to process the line
								    		String columnname = TextAnalysisUtil.normalizeString(columnsnames[column]);
								    		if ( TextAnalysisUtil.isValidTerm(columnname, false) && !(columnname.compareToIgnoreCase("null")==0)) { // if it has a valid column name		    			 
								    			String tupleid = Long.toString(StringHelper.generateMd5(filename + line + columnname));
								    			
								    		 	//loop by words
								    			String wholeSentenceNormalized = TextAnalysisUtil.normalizeString(columns[column]);
												String words[] = TextAnalysisUtil.tokenize(wholeSentenceNormalized);
												
												for (int word=0;word<words.length;word++) { // for each word
													context.getCounter("FCounter", "WordsProcessed").increment(1);
													
													String term = TextAnalysisUtil.normalizeString(words[word]);
													// if the word is a valid term
													if (TextAnalysisUtil.isValidTerm(term)) {
														context.getCounter("FCounter", "ValidTerms").increment(1);
						
														Item item = new Item(term, columnname, line, filename, tupleid);
														
														try {
															context.write(new Text(item.getMapperKey()), new Text(item.getMapperBody())); // word,doc -> columnName,line,ctid
														} catch (InvalidParameterException e) {
															context.getCounter("FError", "MissingMandatoryFields").increment(1);
															logger.error("Could not process file, column and line: " + gzFileName +" - "+ filename + " " + columnname + " " + line );
														}
													} else context.getCounter("FCounter", "InvalidTerm").increment(1);
												} // end of for-loop by words
								    		} else context.getCounter("FCounter", "InvalidColumnName").increment(1); // end of if-valid-column
							    		} else { 
			//								logger.warn("Ignoring line: " + line + " of file " + filename + " .Columns number: "
			//										+ columns.length + ". headers number: "  + columnsnames.length);
							    			context.getCounter("FCounter", "IgnoredLineInvalidColumnsAndHeaders").increment(1);
							    		}
							    	} // end of for-loop by columns 
							    } // end of for-loop by line
							} else {
								logger.warn(" File " + entry.getName()+ " too large: " + entry.getSize());
								System.out.println(" File " + entry.getName()+ " too large: " + entry.getSize());
								context.getCounter("FCounter", "IgnoredFileTooLarge").increment(1);
							}
						} // end of if-csv-file
					
					} else logger.warn("The current entry is a sparse file: " + entry.getName());// end of canReadEntryData
				} catch(Exception e) {
					logger.error("Error inner loop: " + e.getMessage() + ". File: " + gzFileName + " - " + entry.getName());
					System.out.println("Error inner loop: " + e.getMessage() + ". File: " + gzFileName + " - " + entry.getName());
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			logger.error("Error: " + e.getMessage() + ". File: " + gzFileName);
			System.out.println("Error: " + e.getMessage() + ". File: " + gzFileName);
//			entry = tarArchiveInputStream.getNextTarEntry();
//			System.out.println(entry.getName());
		} finally {
		    tarIn.close();
		    gzIn.close();
		    tarArchiveInputStream.close();
		    gzIn.close();
		}

	 }
}
