import java.io.BufferedReader;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.naming.NamingException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

/**
 * Bean implementation class for Enterprise Bean: MongoImportProductManagementData
 */
public class MongoImportProductManagementData

	{

	static final long serialVersionUID = 3206093459760846163L;
	Connection conn;
	//String dbURL = "jdbc:db2://CTSC00579472801.cts.com:50000/labdb:user=wasadmin;password=Pass$007;";
	//String dbURL = "jdbc:derby:D:\\WCSLAB\\WCDE_ENT70\\db\\mall";
	String dbURL = "jdbc:db2://10.242.206.40:50000/wc7dev";
	
	public void myMethod()
    throws javax.naming.NamingException, SQLException
{

   ///////////////////////////////////////////////// 
  //  -- your logic, such as initialization --   // 
  ///////////////////////////////////////////////// 
		List<String> tablelist = new ArrayList<String>();
		MongoClient mongoClient=null;
		String table=null;
		
     try {
    	 System.out.println("Before getting credentials");
    	// MongoCredential credential = MongoCredential.createMongoCRCredential("hari", "test", "admin".toCharArray());
    	// MongoCredential credential = MongoCredential.createMongoCRCredential("admin", "test", "admin".toCharArray());
    	 System.out.println("After getting credentials");
 		 //mongoClient = new MongoClient( new ServerAddress("localhost",27017), Arrays.asList(credential));
    	mongoClient = new MongoClient( new ServerAddress("localhost",27017));
 		System.out.println("After making connection to mongoClient");
 		//DB db = mongoClient.getDB( "wcsdb" );	
 		//DBCollection col = db.getCollection("_schema");
 		//System.out.println(col.find().next().toString());
 		
         // get a connection from the WebSphere Commerce data source
         makeConnection();
         System.out.println("After making connection to DB2");
         BufferedReader br = new BufferedReader(new java.io.FileReader(new File("D:\\Hari\\Hari\\TCG\\MongoDB\\tables1.txt")));
         String line=br.readLine();
         while(line !=null){
        	 tablelist.add(line.trim());
        	 line=br.readLine();
         }
         PreparedStatement stmt = null;    
         
         DB db = mongoClient.getDB( "test" );		
         int i = 0;
         Iterator<String> itr=tablelist.iterator();
         DBCollection col =null;
         int type = 0;
         List<DBObject> bsonlist = new ArrayList<DBObject>();
         while(itr.hasNext())
         {
        	 table=itr.next();
             String query = "select * from " + table +" fetch first 10 rows only with ur" ;
             stmt = conn.prepareStatement(query);    
             /// Delete the MongoDb Collection first to proceed with data insertion

             if (db.collectionExists(table))
             {
                 db.getCollection(table).drop();
             }
                    
             
             ResultSet rs = stmt.executeQuery();
             
           //  stmt.close();
                
             DBObject bson = new BasicDBObject();
             ResultSetMetaData rsmd = rs.getMetaData();
             int numberOfColumns = rsmd.getColumnCount();
             col = db.getCollection(table);
             int ct=0;
             String temp=null;
             while(rs.next())
             {
            	 bson.put("_id", ++ct);
            	 for(int k=1;k<=numberOfColumns;++k){
            		 
            		 type = rsmd.getColumnType(k);
                     if(type == Types.BIGINT || type == Types.INTEGER || type == Types.SMALLINT){
                    	 if(rs.getObject(rsmd.getColumnName(k)) == null){                    	 
                    		 bson.put(rsmd.getColumnName(k), rs.getObject(rsmd.getColumnName(k)));
                    	 }else{
                    		 bson.put(rsmd.getColumnName(k), rs.getLong(rsmd.getColumnName(k)));
                    	 }
                     }else if(type == Types.BOOLEAN){
                    	 if(rs.getObject(rsmd.getColumnName(k)) == null){                    	 
                    		 bson.put(rsmd.getColumnName(k), rs.getObject(rsmd.getColumnName(k)));
                    	 }else{
                    	 bson.put(rsmd.getColumnName(k), rs.getBoolean(rsmd.getColumnName(k)));
                    	 }
                     }else if(type == Types.DATE){
                    	 if(rs.getObject(rsmd.getColumnName(k)) == null){                    	 
                    		 bson.put(rsmd.getColumnName(k), rs.getObject(rsmd.getColumnName(k)));
                    	 }else{
                    	 bson.put(rsmd.getColumnName(k), rs.getDate(rsmd.getColumnName(k)));
                    	 }
                     }else if(type == Types.TIMESTAMP){
                    	 if(rs.getObject(rsmd.getColumnName(k)) == null){                    	 
                    		 bson.put(rsmd.getColumnName(k), rs.getObject(rsmd.getColumnName(k)));
                    	 }else{
                    	 bson.put(rsmd.getColumnName(k), rs.getTimestamp(rsmd.getColumnName(k)));
                    	 }
                     }else if(type == Types.FLOAT){
                    	 if(rs.getObject(rsmd.getColumnName(k)) == null){                    	 
                    		 bson.put(rsmd.getColumnName(k), rs.getObject(rsmd.getColumnName(k)));
                    	 }else{
                    	 bson.put(rsmd.getColumnName(k), rs.getFloat(rsmd.getColumnName(k)));
                    	 }
                     }else if(type == Types.DOUBLE || type == Types.DECIMAL){
                    	 if(rs.getObject(rsmd.getColumnName(k)) == null){                    	 
                    		 bson.put(rsmd.getColumnName(k), rs.getObject(rsmd.getColumnName(k)));
                    	 }else{
                    	 bson.put(rsmd.getColumnName(k), rs.getDouble(rsmd.getColumnName(k)));
                    	 }
                     }else{                         
                    	 temp = rs.getString(rsmd.getColumnName(k));
                    	 if(temp != null) temp = temp.trim();
                    	 bson.put(rsmd.getColumnName(k), temp); 
                     }
            	 }            
            	 col.insert(bson);
            	// bsonlist.add(bson);
             }
             
            // col = db.getCollection(table);
           //  col.insert(bsonlist);  
            // bsonlist.clear();
           //  col=null;
             stmt.close();
             rs.close();
             rs=null;
             stmt=null;
         }
         
        
     }catch(Exception ed){
    	 ed.printStackTrace();
    	 System.out.println("Error importing...........");
    	 }       
     finally {
    	 
       // return the connection to the WebSphere Commerce data source
      closeConnection(); 
     // mongoClient.close();
    }    
}

	private  void closeConnection()
    {
        try
        {
           
            if (conn != null)
            {
                DriverManager.getConnection(dbURL + ";shutdown=true");
                conn.close();
            }           
        }
        catch (SQLException sqlExcept)
        {
            
        }

    }
	
	 private void makeConnection()
	    { 
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
	    }  
	
	 
	 

public static void main(String[] args) {
	MongoImportProductManagementData impData=new MongoImportProductManagementData();
	try {
		impData.myMethod();
	} catch (NamingException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}

}