package br.edu.ufam.icomp.main;

import java.io.File;
import java.net.URI;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import br.edu.ufam.icomp.utils.Constants;
import br.edu.ufam.icomp.utils.StringHelper;

public class Item {
	private static Logger logger = Logger.getLogger(Item.class);
	private String word;
	private String headercolumn;
	private int line=0;
	private URI uri;
	private File csv;
	private String tupleid;
	private int attributeMapping;
	private String filename;
	
	public Item(String word, String headercolumn, int line, String filename, String tupleid) {
		this.word 			= word;
		this.headercolumn 	= headercolumn;
		this.line 			= line;
		this.filename		= filename;
		this.tupleid		= tupleid;
	}
	
	public Item(String word, String headercolumn, int line, URI uri, File csv, String tupleid) {
		this.word 			= word;
		this.headercolumn 	= headercolumn;
		this.line 			= line;
		this.uri 			= uri;
		this.csv			= csv;
		this.tupleid		= tupleid;
	}
	
	public String getWord() {
		return word;
	}
	public void setWord(String word) {
		this.word = word;
	}
	public String getHeadercolumn() {
		return headercolumn;
	}
	public void setHeadercolumn(String headercolumn) {
		this.headercolumn = headercolumn;
	}
	public int getLine() {
		return line;
	}
	public void setLine(int line) {
		this.line = line;
	}

	public URI getUri() {
		return uri;
	}

	public void setUri(URI uri) {
		this.uri = uri;
	}

	public File getCsv() {
		return csv;
	}

	public String getTupleid() {
		return tupleid;
	}

	public void setTupleid(String tupleid) {
		this.tupleid = tupleid;
	}

	public int getAttributeMapping() {
		return attributeMapping;
	}

	public void setAttributeMapping(int attributeMapping) {
		this.attributeMapping = attributeMapping;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}
	
	public String getMapperKey(){
		StringBuilder builder = new StringBuilder();
		builder.append(word);
		builder.append(Constants.TAB);
		builder.append(filename);
		return (builder.toString());
	}
	
	public String getMapperBody() throws InvalidParameterException {
		if ( !StringHelper.isNullOrEmpty(headercolumn) && 
			 !StringHelper.isNullOrEmpty(tupleid) ) { // tupleid tid 
			Map<String, String> itemdetail = new HashMap<String, String>();
			itemdetail.put("hc", headercolumn); // headercolumn hc
			itemdetail.put("l", String.valueOf(line)); // line l
			itemdetail.put("id", tupleid); // tupleid tid
			JSONObject json = new JSONObject(itemdetail);
			return (json.toString());
		} else {
			String errorMessage = "Fiels headercolumn and tupleid cannot be null. Value: " + headercolumn + " " + tupleid;
			logger.error(errorMessage);
			throw new InvalidParameterException(errorMessage);
		}
	}
}
