package com.wmg.bkup;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.cassandra.utils.UUIDGen.decompose;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;
import org.apache.log4j.Logger;
  
public class SSImport
{
    static String filename;
    public static Logger logger = Logger.getLogger(SSImport.class);
    
    static String keyspaceName;
    static String columFamilyName;

    public static void main(String[] args) throws IOException
    {
    	init();
    	filename = args[0].trim();
    	logger.info("keyspaceName -->"+keyspaceName);
    	logger.info("columFamilyName -->"+columFamilyName);
    	
    	
    	//filename = "/home/amila/FYP/cdr_001.csv";
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        //String keyspace = "Demo";
        File directory = new File(keyspaceName);
        if (!directory.exists()){
          directory.mkdir();}
  
        // random partitioner is created, u can give the partitioner as u want
        IPartitioner partitioner = new RandomPartitioner();
  
        SSTableSimpleUnsortedWriter writer = new SSTableSimpleUnsortedWriter(
                directory,partitioner,keyspaceName,columFamilyName,AsciiType.instance,null,64);
     
        String line;
        int lineNumber = 1;
        CsvEntry entry = new CsvEntry();
        // There is no reason not to use the same timestamp for every column in that example.
        long timestamp = System.currentTimeMillis() * 1000;
  
        while ((line = reader.readLine()) != null)
        {
          if (entry.parse(line, lineNumber))
          {
        	logger.info("entry.key "+entry.key.toString());
        	  
        	ByteBuffer uuid = ByteBuffer.wrap(decompose(entry.key));
        	writer.newRow(uuid);
        	writer.addColumn(bytes("firstname"), bytes(entry.firstname), timestamp);
        	writer.addColumn(bytes("lastname"), bytes(entry.lastname), timestamp);
        	writer.addColumn(bytes("password"), bytes(entry.password), timestamp);
        	writer.addColumn(bytes("age"), bytes(entry.age), timestamp);
        	writer.addColumn(bytes("email"), bytes(entry.email), timestamp);
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
        UUID key;
        String firstname;
        String lastname;
        String password;
        long age;
        String email;
  
        boolean parse(String line, int lineNumber)
        {
            // Ghetto csv parsing
            String[] columns = line.split(",");
            if (columns.length != 6)
            {
                System.out.println(String.format("Invalid input '%s' at line %d of %s", line, lineNumber, filename));
                return false;
            }
            try
            {                                                                                                  
                key = UUID.fromString(columns[0].trim());
                firstname = columns[1].trim();
                lastname = columns[2].trim();
                password = columns[3].trim();
                age = Long.parseLong(columns[4].trim());
                email = columns[5].trim();
                
                logger.info("columns[0].trim() "+columns[0].trim());
                logger.info("columns[1].trim() "+columns[1].trim());
                logger.info("columns[2].trim() "+columns[2].trim());
                logger.info("columns[3].trim() "+columns[3].trim());
                logger.info("columns[4].trim() "+columns[4].trim());
                
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