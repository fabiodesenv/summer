package br.edu.ufam.icomp.main.objectdomain;

import br.edu.ufam.icomp.utils.StringHelper;


public class CtidHeader {

	//private static Logger logger = Logger.getLogger(CtidHeader.class);
	String ctid = null;
	String columnsname = null;
	//String columnsnameid = null;
	
	public CtidHeader(String hyphen[], boolean useMD5) {
		ctid = hyphen[0];
		if (useMD5)
			columnsname = StringHelper.generateMd5(hyphen[1]).toString();
		else 
			columnsname = hyphen[1];
	}

	public String getCtid() {
		return ctid;
	}

	public void setCtid(String ctid) {
		this.ctid = ctid;
	}

	public String getColumnsname() {
		return columnsname;
	}

	public void setColumnsname(String columnsname) {
		this.columnsname = columnsname;
	}
	
	
}
