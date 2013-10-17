package com.wmg.bkup;

import java.nio.ByteBuffer;
import java.io.*;
import java.util.UUID;
  
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.cassandra.utils.UUIDGen.decompose;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.log4j.Logger;
  
public class DataImport
{
    static String filename;
    public static Logger logger = Logger.getLogger(DataImport.class);
    
    public static void main(String[] args) throws IOException
    {
    	String keyspace = args[0].trim();
    	String COLUMN_FAMILY_NAME = args[1].trim();
    	filename = args[2].trim();
    	
    	logger.info("keyspace coming from args[0] -->"+keyspace);
    	logger.info("COLUMN_FAMILY_NAME from args[1] -->"+COLUMN_FAMILY_NAME);
    	
    	
    	if("MusicMetricData".equals(keyspace)){
    		logger.info("MusicMetricData is the value from args[0]");
    	}
    	if("Users".equals(COLUMN_FAMILY_NAME)){
    		logger.info("Users is the value from args[1]");
    	}

    	keyspace="MusicMetricData";
    	COLUMN_FAMILY_NAME="Users";

    	logger.info("setting keyspace-->"+keyspace.trim());
    	logger.info("setting COLUMN_FAMILY_NAME-->"+COLUMN_FAMILY_NAME.trim());
    	
    	//filename = "/home/amila/FYP/cdr_001.csv";
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        //String keyspace = "Demo";
        File directory = new File(keyspace);
        if (!directory.exists()){
          directory.mkdir();}
  
        // random partitioner is created, u can give the partitioner as u want
        IPartitioner partitioner = new RandomPartitioner();
  
        SSTableSimpleUnsortedWriter writer = new SSTableSimpleUnsortedWriter(
                directory,partitioner,keyspace,COLUMN_FAMILY_NAME,AsciiType.instance,null,64);
     
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