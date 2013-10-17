package com.wmg;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;
import org.apache.log4j.Logger;
  
public class SSTableImport
{
    static String filename;
    public static Logger logger = Logger.getLogger(SSTableImport.class);
    
    static String keyspaceName;
    static String columFamilyName;

    public static void main(String[] args) throws IOException
    {
    	init();
    	filename = args[0].trim();
    	logger.info("keyspaceName -->"+keyspaceName);
    	logger.info("columFamilyName -->"+columFamilyName);
    	
/*		GZIPInputStream zipReader = new GZIPInputStream(new FileInputStream("musicmetric-facebook-data.tsv.gz"));
		InputStreamReader streamReader = new InputStreamReader(zipReader);
		BufferedReader br = new BufferedReader(streamReader);
*/
    	BufferedReader reader = new BufferedReader(new FileReader(filename));
        File directory = new File(keyspaceName);
        if (!directory.exists()){
          directory.mkdir();}
  
        // random partitioner is created, u can give the partitioner as u want
        IPartitioner partitioner = new RandomPartitioner();
  
        SSTableSimpleUnsortedWriter writer = new SSTableSimpleUnsortedWriter(
                directory,partitioner,keyspaceName,columFamilyName,UTF8Type.instance,null,64);
     
        String line;
        int lineNumber = 1;
        CsvEntry entry = new CsvEntry();
        // There is no reason not to use the same timestamp for every column in that example.
        long timestamp = System.currentTimeMillis() * 1000;
  
        while ((line = reader.readLine()) != null)
        {
            if (entry.parse(line, lineNumber))
            {
            	
            	writer.newRow(bytes(entry.key));
            	writer.addColumn(bytes("value"), bytes(entry.value), timestamp);
            }
            lineNumber++;
        }
        // Don't forget to close!
        writer.close();
        
        System.exit(0);
    }
  
    public static void init(){
		Properties properties = new Properties();
		try {
			properties.load(new FileInputStream("properties/sstable.properties"));
		    
			keyspaceName = properties.getProperty("keyspaceName");
		    columFamilyName = properties.getProperty("columnFamilyName");

		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
    }

    static class CsvEntry
    {
        String key;
        String value;
  
        boolean parse(String line, int lineNumber)
        {
            // Ghetto csv parsing
            String[] columns = line.split("\\t");
            if (columns.length != 2)
            {
                System.out.println(String.format("Invalid input '%s' at line %d of %s", line, lineNumber, filename));
                return false;
            }
            try
            {                                                                                                  
            	key = columns[0].trim();
                value = columns[1].trim();
                
                logger.info("key "+columns[0].trim());
                logger.info("value "+columns[1].trim());
                
                return true;
            }
            catch (NumberFormatException e)
            {
                System.out.println(String.format("Invalid number in input '%s' at line %d of %s", line, lineNumber, filename));
                return false;
            }
        }
    }
}