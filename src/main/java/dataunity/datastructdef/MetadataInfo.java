package dataunity.datastructdef;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import dataunity.qb.Component;
import dataunity.qb.ComponentSpecification;
import dataunity.qb.DataStructureDefinition;
import dataunity.qb.DataStructureDefinitionSerializer;
import dataunity.qb.Dimension;
import au.com.bytecode.opencsv.CSVReader;

import com.hp.hpl.jena.rdf.model.Model;

public class MetadataInfo {
	private static Pattern decimalPattern = Pattern.compile("^(\\d)+\\.(\\d)+$");
	private static Pattern integerPattern = Pattern.compile("^(\\d)+$");
	
	public static boolean isDecimal(String str) {
		if (str == null) {
			return false;
		}
		Matcher m = decimalPattern.matcher(str);
		return m.matches();
	}
	
	public static boolean isInteger(String str) {
		if (str == null) {
			return false;
		}
		Matcher m = integerPattern.matcher(str);
		return m.matches();
	}
	
	private static String guessXSDType(Collection<String> vals) {
		Set<String> types = new HashSet<String>();
		String xsdPrefix = "http://www.w3.org/2001/XMLSchema#";
		String xsdString = xsdPrefix + "string";
		String xsdDecimal = xsdPrefix + "decimal";
		String xsdInteger = xsdPrefix + "integer";
		List<String> decimalTypes = Arrays.asList("decimal", "integer");
		String item;
		Iterator<String> iter = vals.iterator();
		while (iter.hasNext()) {
			item = iter.next();
			if (isDecimal(item)){
				types.add("decimal");
			}
			else if (isInteger(item)){
				types.add("integer");
			}
			else {
				types.add("string");
			}
		}
		
		if (types.isEmpty()) {
			return xsdString;
		}
		else {
			if (types.contains("string")) {
				return xsdString;
			}
			else if (types.size() == 1 && types.contains("integer")) {
				return xsdInteger;
			}
			else if (types.size() == 2 && types.containsAll(decimalTypes)) {
				return xsdDecimal;
			}
			else {
				return xsdString;
			}
		}
	}
	
	public static Model extractDataStructDef(String dataUnityBaseURL, String path, String encoding) throws IOException {
		int lineIndex = 0;
		InputStreamReader streamReader = null;
		if (encoding == null) {
			streamReader = new InputStreamReader(new FileInputStream(path));
		}
		else {
			streamReader = new InputStreamReader(new FileInputStream(path), encoding);
		}
		CSVReader reader = new CSVReader(streamReader, ',', '\"', 0);
	    String [] nextLine;
	    String[] headers;
	    String val;
	    Map<Integer, String> colPositionToColName = new HashMap<Integer, String>();
	    Map<Integer, ArrayList<String>> colPositionToColValues = new HashMap<Integer, ArrayList<String>>();
	    while ((nextLine = reader.readNext()) != null) {
	    	for (int i = 0; i < nextLine.length; i++) {
	    		val = nextLine[i];
	    		if (lineIndex == 0) {
		        	// Store headers
	    			colPositionToColName.put(i, val);
		        }
	    		else {
	    			// Store values
	    			if (!colPositionToColValues.containsKey(i)) {
	    				colPositionToColValues.put(i, new ArrayList<String>());
	    			}
	    			colPositionToColValues.get(i).add(val);
	    		}
	    	}
	    	lineIndex++;
	    }
	    
	    Map<Integer, String> colIndexToDataType = new HashMap<Integer, String>();
	    Set<Integer> keys = colPositionToColValues.keySet();
	    Iterator<Integer> iter = keys.iterator();
	    while (iter.hasNext()) {
	    	Integer key = iter.next();
	    	ArrayList<String> vals = colPositionToColValues.get(key);
	    	String dataType = guessXSDType(vals);
	    	colIndexToDataType.put(key, dataType);
	    }
	    
//	    Iterator<Integer> tmpIter = colIndexToDataType.keySet().iterator();
//	    while (tmpIter.hasNext()) {
//	    	Integer key = tmpIter.next();
//	    	System.out.println(key + " " + colPositionToColName.get(key) + " " + colIndexToDataType.get(key));
//	    }
	    
	    // Build info in DataStructureDefinition type structure
	    DataStructureDefinition dataStructDef = new DataStructureDefinition();
	    Iterator<Entry<Integer, String>> iterColPosName = colPositionToColName.entrySet().iterator();
	    while (iterColPosName.hasNext()) {
	    	Entry<Integer, String> item = iterColPosName.next();
	    	Integer colPosition = item.getKey();
	    	String colName = item.getValue();
	    	String dataType = colIndexToDataType.get(colPosition);
	    	Dimension dim = new Dimension(colName, dataType);
	    	ComponentSpecification compSpec = new ComponentSpecification(colName, dim);
	    	Component comp = new Component(compSpec);
	    	dataStructDef.getComponent().add(comp);
	    }
	    
	    // Create turtle
	    DataStructureDefinitionSerializer rdfSerialiser = new DataStructureDefinitionSerializer(dataUnityBaseURL);
	    Model model = rdfSerialiser.toRDF(dataStructDef);
	    
	    return model;
	}
}
