package br.edu.ufam.icomp.main.objectdomain;

import java.util.HashSet;
import java.util.Set;

import br.edu.ufam.icomp.utils.StringHelper;

public class Element {
	//private static Logger logger = Logger.getLogger(Element.class);
	private String csvname = null;
	//private String csvnameid = null;
	private long freq = 0;
	private Set<CtidHeader> ctidH = new HashSet<CtidHeader>();
	
	public String getCsvname() {
		return csvname;
	}

	public void setCsvname(String csvname) {
		this.csvname = csvname;
	}

	public long getFreq() {
		return freq;
	}

	public void setFreq(long freq) {
		this.freq = freq;
	}

	public Set<CtidHeader> getCtidH() {
		return ctidH;
	}

	public void setCtidH(Set<CtidHeader> ctidH) {
		this.ctidH = ctidH;
	}

	public Element(String comma[], boolean useMD5) {
		if (useMD5)
			csvname = StringHelper.generateMd5(comma[0]).toString();
		else
			csvname = comma[0];
		try {
			freq = Long.valueOf(comma[1]);
		} catch (Exception e) {
			System.out.println("Error. Not possible to get freq from element: " + comma[0] + " , " + comma[1]);
		}
		
		String sCtid = "";
		String sHeader = "";
		for (int i=2;i<comma.length;i++) {					
			if (sCtid.compareTo("")==0) {
				sCtid = comma[i];
			} else {
				String[] composto = findColumnName(comma[i]);
				sHeader = composto[0];
				
				String[] ctidheader = {sCtid, sHeader};
				CtidHeader ctid = new CtidHeader(ctidheader, useMD5);
				ctidH.add(ctid);
				
				
				// ########### ATENCAO - ERRADO ###########################################@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
//				if (ctidH.size()>1) // TODO ERROR ERRADO se um termo aparece mais de uma vez na mesma tabela, o que fazer? se for na mesma coluna e nao for composto, posso deletar?
//					break;
				// ########### ATENCAO ERRADO ###########################################@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
				
				if ( !StringHelper.isNullOrEmpty(composto[1]) )
					sCtid = composto[1];
			}
		}
	}
	
	private static String[] findColumnName(String str) {
		String[] result = {str,""};
		if (str.indexOf("-")>=0) {
			int i = 0;
			while ((i = str.indexOf("-",i+1)) !=-1 ) {
				result[0] = str.substring(0,i);
			}
			
			if (result[0].lastIndexOf("-")==result[0].length()-1) {
				result[0] = result[0].substring(0, result[0].length()-1);
				result[1] = str.substring(str.lastIndexOf("-"), str.length());
			} else
				result[1] = str.substring(str.lastIndexOf("-")+1, str.length());
		}
		return result;
	}
}
