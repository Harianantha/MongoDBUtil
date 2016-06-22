package com.cognizant.mongodb.util;

import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class DBUtil {
	private static String dbURL = "jdbc:db2://10.242.206.40:50000/wc7dev";
	public static MongoClient getMongoDBClient() throws UnknownHostException{
		MongoClient mongoClient=null;
		mongoClient = new MongoClient( new ServerAddress("127.0.0.1",27017));
		return mongoClient;
	}
	
	 public static Connection makeDb2Connection()
	    { 
		 Connection conn=null;
	        try
	        {
	            Class.forName("com.ibm.db2.jcc.DB2Driver").newInstance();
	            //Class.forName("com.ibm.db2.jcc.DB2Driver").newInstance();
	            //Get a connection
	            conn = DriverManager.getConnection(dbURL,"wcsadmin","password-1"); 
	           
	        }
	        catch (Exception except)
	        {
	            except.printStackTrace();
	        }
	        return conn;
	    }  
	
	 public static  void closeConnection(Connection conn)
	    {
	        try
	        {
	           
	            if (conn != null)
	            {
	              //  DriverManager.getConnection(dbURL + ";shutdown=true");
	                conn.close();
	            }           
	        }
	        catch (SQLException sqlExcept)
	        {
	            
	        }

	    }

}
