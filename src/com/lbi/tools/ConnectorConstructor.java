
package com.lbi.tools;

import java.io.*;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.TimeZone;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;

import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class ConnectorConstructor
{
	ConnectorDetsLoader cdl;
	
	Logger logger = Logger.getLogger(ConnectorConstructor.class.getName());

	public ConnectorConstructor() throws Exception
	{
		this.cdl = new ConnectorDetsLoader("/home/dinesh_lbi/LBI/lbi/app/conf/ConnectorDetails.xml");
	}

	public static void main(String arg[]) throws Exception
	{
		String connector = "freshdesk";
		ConnectorConstructor cc = new ConnectorConstructor();
		cc.updateConnDetsAndSaveAs(connector, "/home/dinesh_lbi/LBI/lbiconndetails.xml");
		cc.constructTableQueries(connector, "/home/dinesh_lbi/LBI/lbitablequeries.sql");
		cc.constructReferablesList(connector, "/home/dinesh_lbi/LBI/lbireferables.txt");
	}

	public void constructReferablesList(String connector, String file) throws Exception
	{
		cdl.setConnector(connector);
		FileWriter fw = new FileWriter(file, false);
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write("//  ##################     " + connector.substring(0, 1).toUpperCase() + connector.substring(1) + " Connector Referables List    ###############");
		bw.newLine();
		bw.newLine();
		String modName, fldName, referable; Element module, field;
		LinkedHashMap<String, Element> modules = cdl.getModuleDets(), fields;
		HashMap<String, LinkedHashMap<String, Element>> modVsFldDets= cdl.getModVsFldDets();
	    for(Entry<String, Element> modEnry : modules.entrySet())
	    {
	    	modName = modEnry.getKey();
	    	module = modEnry.getValue();
	    	fields = modVsFldDets.get(modName);
	        for(Entry<String, Element> fldEntry : fields.entrySet())
	        {
	        	fldName = fldEntry.getKey();
	        	field = fldEntry.getValue();
				referable = field.getAttribute("matchingcol");
				if (referable.equalsIgnoreCase("true"))
				{
					bw.write("refmod=\"" + modName + "\" reffld=\"" + fldName + "\" ");
					bw.newLine();
					bw.newLine();
				}
			}
		}
		bw.close();
	}
	
	public void constructTableQueries(String conn, String file) throws Exception
	{
		cdl.setConnector(conn);
		FileWriter fw = new FileWriter(file, true);
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write("-- " + conn.substring(0, 1).toUpperCase() + conn.substring(1) + " Connector Tables");
		bw.newLine();
		bw.newLine();
		Element module, field;
		boolean isPrimary, notnull;
		StringBuilder creTbl, addUniqs;
		Entry<String, Element> fldEntry;
		String modName, fldName, datatype;
		Iterator<Entry<String, Element>> it;
		Element connector = cdl.getConnector();
		LinkedHashMap<String, Element> modules = cdl.getModuleDets(), fields;
		HashMap<String, LinkedHashMap<String, Element>> modVsFldDets= cdl.getModVsFldDets();
	    for(Entry<String, Element> modEnry : modules.entrySet())
	    {
			int count = 0;
	    	modName = modEnry.getKey();
	    	module = modEnry.getValue();
	    	fields = modVsFldDets.get(modName);
	    	
//	    	bw.write("drop table " + modName + ";");
//			bw.newLine();
//			bw.newLine();
//	    	if(module.hasAttribute("historytable"))
//	    	{
//	    	bw.write("drop table " + module.getAttribute("historytable") + ";");
//			bw.newLine();
//			bw.newLine();
//	    	}
	    	
//	    	bw.write("delete from " + modName + ";");
//			bw.newLine();
//			bw.newLine();
//	    	if(module.hasAttribute("historytable"))
//	    	{
//	    	bw.write("delete from " + module.getAttribute("historytable") + ";");
//			bw.newLine();
//			bw.newLine();
//	    	}
	    	
//	    	bw.write("select * from " + modName + ";");
//			bw.newLine();
//			bw.newLine();
//	    	if(module.hasAttribute("historytable"))
//	    	{
//	    	bw.write("select * from " + module.getAttribute("historytable") + ";");
//			bw.newLine();
//			bw.newLine();
//	    	}
	    	
	    	creTbl = new StringBuilder();
	    	addUniqs = new StringBuilder(" add unique uniq10 (");
	    	it = fields.entrySet().iterator();
	        while(it.hasNext())
	        {
	        	count++;
	        	fldEntry = it.next();
	        	fldName = fldEntry.getKey();
	        	field = fldEntry.getValue();
				datatype = field.getAttribute("datatype");
				isPrimary = Boolean.valueOf(field.getAttribute("matchingcol"));
				notnull = Boolean.valueOf(field.getAttribute("notnull"));
				creTbl.append(fldName);
				addUniqs.append(fldName);
				if (datatype.equalsIgnoreCase("Int"))
				{
					creTbl.append(" int");
				}
				else if (datatype.equalsIgnoreCase("Float"))
				{
					creTbl.append(" float");
				}
				else if (datatype.equalsIgnoreCase("Long"))
				{
					creTbl.append(" bigint");
				}
				else if (datatype.equalsIgnoreCase("Double"))
				{
					creTbl.append(" double");
				}
				else if (datatype.equalsIgnoreCase("String"))
				{
					creTbl.append(" varchar(100)");
				}
				else if (datatype.equalsIgnoreCase("Date"))
				{
					creTbl.append(" datetime");
				}
				else if (datatype.equalsIgnoreCase("Boolean"))
				{
					creTbl.append(" tinyint(1)");
				}
				if(isPrimary)
				{
					creTbl.append(" primary key");
				}
				else if(notnull)
				{
					creTbl.append(" not null");
				}
				if (field.hasAttribute("refmod"))
				{
					creTbl.append(" references " + field.getAttribute("refmod") + " (" + field.getAttribute("reffld") + ")");
				}
				if(it.hasNext())
				{
					creTbl.append(", ");
					if((count % 10) == 0)
					{
						addUniqs.append("), add unique uniq" + (count + 10) + " (");
					}
					else
					{
						addUniqs.append(", ");
					}
				}
				else
				{
					creTbl.append(");");
					addUniqs.append(");");
				}
			}
	        bw.write("create table " + modName + " (userid bigint not null, connid bigint not null, " + creTbl.toString());
			bw.newLine();
			bw.newLine();
			if(module.hasAttribute("historytable"))
	    	{
		        bw.write("create table " + module.getAttribute("historytable") + " (historyid bigint primary key auto_increment, userid bigint, connid bigint, " + creTbl.toString().replaceAll(" primary key", ""));
				bw.newLine();
				bw.newLine();
				bw.write("alter table " + module.getAttribute("historytable") + addUniqs.toString());
				bw.newLine();
				bw.newLine();
	    	}
		}
		bw.close();
	}
	
	public void updateConnDetsAndSaveAs(String connector, String file) throws Exception
	{
		cdl.setConnector(connector);
		String modName, fldName; Element module, field;
		LinkedHashMap<String, Element> modules = cdl.getModuleDets(), fields;
		HashMap<String, LinkedHashMap<String, Element>> modVsFldDets= cdl.getModVsFldDets();
	    for(Entry<String, Element> modEnry : modules.entrySet())
	    {
	    	modName = modEnry.getKey();
	    	module = modEnry.getValue();
	    	fields = modVsFldDets.get(modName);
	        for(Entry<String, Element> fldEntry : fields.entrySet())
	        {
	        	fldName = fldEntry.getKey();
	        	field = fldEntry.getValue();
	    		TransformerFactory transformerFactory = TransformerFactory.newInstance();
	    		Transformer transformer = transformerFactory.newTransformer();
	    		transformer.transform(new DOMSource(cdl.doc), new StreamResult(file));
	        }
	    }
	}
	
}
