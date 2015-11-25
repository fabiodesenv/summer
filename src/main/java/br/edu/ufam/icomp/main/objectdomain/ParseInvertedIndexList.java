package br.edu.ufam.icomp.main.objectdomain;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import br.edu.ufam.icomp.main.Item;
import br.edu.ufam.icomp.utils.StringHelper;


public class ParseInvertedIndexList {
	private static Logger logger = Logger.getLogger(ParseInvertedIndexList.class);
	private String word;
	private double idf;
	private List<Element> element = new ArrayList<Element>();
	long totalelements = 0;
	
	public ParseInvertedIndexList(String s, boolean useMD5) {
		String tabs[] = s.split("\t");
		String firstpart = tabs[0];
		
		word = firstpart.split(",")[0];		
		
		try {
			idf = Double.parseDouble(firstpart.split(",")[1]);
			totalelements = Long.valueOf(tabs[2]);
		} catch (Exception e) {
			System.out.println("Error. Not possible to get total of elements");
		}
		
		//System.out.println("first: " + firstpart + "\t" + totalelements);
		
		String pipes[] = tabs[1].split("\\|");
		int counter = 0;
		//System.out.println("Progress: ");
		//Float percentage = 0.3F;
		//int limit = Math.round(totalelements*percentage);
		for (String str : pipes) {
			if (!StringHelper.isNullOrEmpty(str)) {
				String comma[] = str.split(",");
				if (comma.length>=4) {
					Element e = new Element(comma, useMD5);
					element.add(e);
				} else logger.error("Not a valid element. Size: " + comma.length + ". String: " + str);
			}
			counter++;
			
			// TODO remove this line, temporary, only for tests
			//if (counter >= limit) break;
			if (counter%100000==0) {
	        	System.out.print(counter*100/pipes.length + "% ");
	        }
		}
		//System.out.println("");
	}
	
	public List<Item> transformToItens() throws URISyntaxException {
		List<Item> result = new ArrayList<Item>();
		
		for (Element e : element) {
			for (CtidHeader c : e.getCtidH()) {
				URI uri = null;
				try {
					long fileName = StringHelper.generateMd5(e.getCsvname());
					uri = new URI("file://" + Long.toString(fileName));
				} catch (URISyntaxException e1) {
					uri = new URI("a.com");
					logger.error("Not able to parse filename into URI: " + e.getCsvname());
				}
			
				long columnName = StringHelper.generateMd5(c.getColumnsname());
				Item i = new Item(word,Long.toString(columnName),0,uri,null,c.getCtid());
				result.add(i);
			}
			e.getCtidH().clear();
		}
		element.clear();
		return result;
	}


	public String getWord() {
		return word;
	}


	public void setWord(String word) {
		this.word = word;
	}


	public double getIdf() {
		return idf;
	}


	public void setIdf(double idf) {
		this.idf = idf;
	}


	public List<Element> getElement() {
		return element;
	}


	public void setElement(List<Element> element) {
		this.element = element;
	}


	public long getTotalelements() {
		return totalelements;
	}


	public void setTotalelements(long totalelements) {
		this.totalelements = totalelements;
	}
}
