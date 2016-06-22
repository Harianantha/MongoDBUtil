package com.cognizant.mongodb;
import java.io.BufferedReader;
import java.io.File;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.ejb.SessionContext;
import javax.naming.NamingException;

import com.cognizant.mongodb.util.DBUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

/**
 * Bean implementation class for Enterprise Bean: MongoImportProductManagementData
 */
public class MongoImportCatentryAndClassicAttributes

	{

	static final long serialVersionUID = 3206093459760846163L;
	
	//String dbURL = "jdbc:db2://CTSC00579472801.cts.com:50000/labdb:user=wasadmin;password=Pass$007;";
	//String dbURL = "jdbc:derby:D:\\WCSLAB\\WCDE_ENT70\\db\\mall";
	
	
	public void insertCatentry()
    throws javax.naming.NamingException, SQLException
{

   ///////////////////////////////////////////////// 
  //  -- your logic, such as initialization --   // 
  ///////////////////////////////////////////////// 
	
		MongoClient mongoClient=null;
		String table="CATENTRY";
		Connection conn=null;
		
     try {
    	 System.out.println("Before getting credentials");
    	// MongoCredential credential = MongoCredential.createMongoCRCredential("hari", "test", "admin".toCharArray());
    	// MongoCredential credential = MongoCredential.createMongoCRCredential("admin", "test", "admin".toCharArray());
    	 System.out.println("After getting credentials");
 		 //mongoClient = new MongoClient( new ServerAddress("localhost",27017), Arrays.asList(credential));
    	mongoClient =DBUtil.getMongoDBClient();
 		System.out.println("After making connection to mongoClient");
 		
 		
         // get a connection from the WebSphere Commerce data source
 		conn=DBUtil.makeDb2Connection();
         System.out.println("After making connection to DB2");
         
         PreparedStatement stmt = null;    
         
         DB db = mongoClient.getDB( "test" );		
         int i = 0;
     
         DBCollection col =null;
         int type = 0;
         List<DBObject> bsonlist = new ArrayList<DBObject>();
        
    	 
         String query = "SELECT  CAT.CATENTRY_ID,CAT.PARTNUMBER,CAT.MFPARTNUMBER,CAT.FIELD1,CAT.FIELD2, CAT.URL,CAT.LASTUPDATE,CAT.FIELD3,CAT.FIELD4,CAT.STARTDATE,CAT.ENDDATE,CAT.AVAILABILITYDATE,CAT.DISCONTINUEDATE,CAT.UP_MFNAME,CAT.UP_MFPARTNUMBER,CDESC.LANGUAGE_ID,CDESC.NAME,CDESC.SHORTDESCRIPTION,CDESC.LONGDESCRIPTION,CDESC.THUMBNAIL,CDESC.AUXDESCRIPTION1,CDESC.FULLIMAGE,CDESC.AUXDESCRIPTION2,CDESC.XMLDETAIL,CDESC.AVAILABLE,CDESC.AVAILABILITYDATE,CDESC.KEYWORD,CDESC.UP_NAME FROM CATENTRY CAT,CATENTDESC CDESC WHERE CAT.MARKFORDELETE = 0 and CAT.BUYABLE = 1 and CDESC.PUBLISHED=1 and CAT.catentry_id=CDESC.catentry_id AND CAT.CATENTTYPE_ID='ProductBean' FETCH FIRST 1000 ROWS ONLY WITH UR";
         stmt = conn.prepareStatement(query);    
         /// Delete the MongoDb Collection first to proceed with data insertion

        /* if (db.collectionExists(table))
         {
             db.getCollection(table).drop();
         }
                */
         
         ResultSet rs = stmt.executeQuery();
         
       //  stmt.close();
            
         DBObject bson =null;
         ResultSetMetaData rsmd = rs.getMetaData();
         int numberOfColumns = rsmd.getColumnCount();
         col = db.getCollection(table);
         int ct=0;
         String temp=null;
         while(rs.next())
         {
        	 System.out.println("Count is:+ct");
        	 bson= new BasicDBObject();
        	 bson.put("_id", ++ct);
        	 for(int k=1;k<=numberOfColumns;++k){
        		 
        		 populateBSONfromResultSet(rs, bson, rsmd, k);
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
         
         
        
     }catch(Exception ed){
    	 ed.printStackTrace();
    	 System.out.println("Error importing...........");
    	 }       
     finally {
    	 
       // return the connection to the WebSphere Commerce data source
      DBUtil.closeConnection(conn); 
     // mongoClient.close();
    }    
}

	private void populateBSONfromResultSet(ResultSet rs, DBObject bson,
			ResultSetMetaData rsmd, int k) throws SQLException {
		int type;
		String temp;
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

private void updateCatentriesWithAttributes() throws UnknownHostException, SQLException{
	
	//String mongoquery="test.CATENTRY.find( {\"attributes\":\"null\"})";
	String mongoquery="test.CATENTRY.find()";
	MongoClient mongoClient=null;
	mongoClient =DBUtil.getMongoDBClient();
	DB db = mongoClient.getDB( "test" );
	DBCollection collection = db.getCollection("CATENTRY");
	DBObject bson =  new BasicDBObject( "ATTRIBUTES",null);
	//DBObject bson =  new BasicDBObject("_id",1);
	BasicDBObject query=new BasicDBObject("CATENTRY_ID",181786);
	//CommandResult result=db.command(bson);
	/*CommandResult result=db.command("test.CATENTRY.find()");
	System.out.println(result.size());*/
	DBCursor cursor=collection.find(bson);
	/*System.out.println(result.size());
	System.out.println(result.toMap());*/
	//while(r)
	System.out.println(cursor.size());
	Long catentryId=null;
	Connection connection=DBUtil.makeDb2Connection();
	String attributesquery="select at.attribute_id,at.LANGUAGE_ID,at.name,at.DESCRIPTION,at.DESCRIPTION2,at.FIELD1,at.QTYUNIT_ID,av.ATTRVALUE_ID,av.STRINGVALUE,av.NAME as avname,av.FIELD1,av.IMAGE1,av.IMAGE2,av.FIELD2,av.FIELD3,av.ATTACHMENT_ID from attribute at,attrvalue av where at.catentry_id=? and at.catentry_id=av.catentry_id and at.attribute_id=av.ATTRIBUTE_ID and at.LANGUAGE_ID=av.LANGUAGE_ID and av.STRINGVALUE is not null with ur";
	while(cursor.hasNext()){
		//System.out.println(cursor.next().get("NAME"));
		DBObject attributeDBObject=cursor.next();
		catentryId=(Long)attributeDBObject.get("CATENTRY_ID");
		PreparedStatement st=connection.prepareStatement(attributesquery);
		st.setDouble(1, catentryId.intValue());
		ResultSet rs= st.executeQuery();
		
		ResultSetMetaData rsmd = rs.getMetaData();
        int numberOfColumns = rsmd.getColumnCount();
      //  BasicDBObject attributeValuesArrayBSON = new BasicDBObject();	
        BasicDBObject attributeValuesListBSON = new BasicDBObject();	
        while (rs.next()){
        	DBObject attributeValuesBSON = new BasicDBObject();	
        	for(int k=1;k<=numberOfColumns;++k){
        		 
        		 populateBSONfromResultSet(rs, attributeValuesBSON, rsmd, k);
        	 }    
        	attributeValuesListBSON.append(rs.getString("ATTRVALUE_ID"), attributeValuesBSON);
        }
       // attributeValuesArrayBSON.append("attributes", attributeValuesListBSON);
        attributeDBObject.put("attributes", attributeValuesListBSON);
        collection.save(attributeDBObject);
        
	}
}



	
	
	 

public static void main(String[] args) {
	MongoImportCatentryAndClassicAttributes impData=new MongoImportCatentryAndClassicAttributes();
	
	try {
			long starttime=System.currentTimeMillis();
			impData.insertCatentry();
	
			impData.updateCatentriesWithAttributes();
			long endtime=System.currentTimeMillis();
			System.out.println("Time taken to insert ctaentry and attributes:"+(endtime-starttime)+"millis");
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	/* catch (NamingException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}*/ catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NamingException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}

}