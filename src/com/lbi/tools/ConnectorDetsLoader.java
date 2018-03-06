package com.lbi.tools;

import java.io.File;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.logging.Level;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class ConnectorDetsLoader
{
  
	private File xmlFile;
	Document doc;
	XPath xpath;
	String connector;
	public long fetchTime;

	public ConnectorDetsLoader(String filePath)
	{
		try
		{   
			xmlFile = new File(filePath);
			DocumentBuilderFactory dom = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = dom.newDocumentBuilder();
			this.doc = docBuilder.parse(xmlFile);
			XPathFactory xpathFactory = XPathFactory.newInstance();
			this.xpath = xpathFactory.newXPath();
			this.fetchTime = System.currentTimeMillis();
		}
		catch(Exception e)
	    {
			System.out.println("Default Initialization Failed ::: "+e.getMessage());
	    }
	}

	public void setConnector(String connector)
	{
		this.connector = connector;
	}
	
	public String constructXPathExprForConnectors()
	{
		return "connectors/connector"; //NO I18N
	}

	public String constructXPathExprForConnector()
	{
		return "connectors/connector[@metaname=\"" + connector + "\"]"; //NO I18N
	}
	  
	public String constructXPathExprForModules()
	{
		return "connectors/connector[@metaname=\"" + connector + "\"]/modules/module"; //NO I18N
	}
	 
	public String constructXPathExprForModule(String module)
	{
		return "connectors/connector[@metaname=\"" + connector + "\"]/modules/module[@tablename=\"" + module + "\"]"; //NO I18N
	}
	
	public String constructXPathExprForFields(String module)
	{
		return "connectors/connector[@metaname=\"" + connector + "\"]/modules/module[@tablename=\"" + module + "\"]/fields/field";        //NO I18N
	}
	
	public LinkedHashMap<String, Element> getModuleDets() throws Exception
	{
		LinkedHashMap<String, Element> modDets = new LinkedHashMap<String, Element>();
	    XPathExpression xpathExprMod = xpath.compile(constructXPathExprForModules());
	    NodeList moduleSet = (NodeList) xpathExprMod.evaluate(doc, XPathConstants.NODESET);
        Element module; String modName; Node node;
	    for(int i = 0; i < moduleSet.getLength(); i++)
	    {
	    	node = moduleSet.item(i);
	        module = (Element) node;
	        modName = module.getAttribute("tablename");
	        modDets.put(modName, module);
	    }
	    return modDets;
	}
	
	public HashMap<String, LinkedHashMap<String, Element>> getModVsFldDets() throws Exception
	{
		HashMap<String, LinkedHashMap<String, Element>> modVsFldDets = new HashMap<String, LinkedHashMap<String, Element>>();
		LinkedHashMap<String, Element> fldDets; XPathExpression xpathExprField; NodeList fields; Node node;
		XPathExpression xpathExprMod = xpath.compile(constructXPathExprForModules());
	    NodeList moduleSet = (NodeList) xpathExprMod.evaluate(doc, XPathConstants.NODESET);
	    String modName, fldName; Element module, field;
	    for(int i = 0; i < moduleSet.getLength(); i++)
	    {
	    	fldDets = new LinkedHashMap<String, Element>();
	        node = moduleSet.item(i);
	        module = (Element) node;
	        modName = module.getAttribute("tablename");
	        xpathExprField = xpath.compile(constructXPathExprForFields(modName));
	        fields = (NodeList) xpathExprField.evaluate(doc, XPathConstants.NODESET);
	        for(int j = 0; j < fields.getLength(); j++)
	        {
	        	node = fields.item(j);
	            field = (Element) node;
	            fldName = field.getAttribute("metaname");
	            fldDets.put(fldName, field);
	        }
	        modVsFldDets.put(modName, fldDets);
	    }
	    return modVsFldDets;
	}
	  
	public ArrayList<Element> getListOfConnectors() throws Exception
	{
		ArrayList<Element> list = new ArrayList<Element>();
		XPathExpression xpathExprMod = xpath.compile(constructXPathExprForConnectors());
	    NodeList moduleSet = (NodeList) xpathExprMod.evaluate(doc, XPathConstants.NODESET);
	    for(int i = 0; i < moduleSet.getLength(); i++)
	    {
	    	Node node = moduleSet.item(i);
	    	list.add(((Element) node));
	    }
		return list;
	}
	  
	public Element getConnector() throws Exception
	{
		ArrayList<Element> list = new ArrayList<Element>();
		XPathExpression xpathExprMod = xpath.compile(constructXPathExprForConnector());
	    NodeList moduleSet = (NodeList) xpathExprMod.evaluate(doc, XPathConstants.NODESET);
		return (Element)moduleSet.item(0);
	}
	
}
