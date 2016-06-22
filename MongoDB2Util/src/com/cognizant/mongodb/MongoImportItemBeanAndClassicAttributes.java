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
public class MongoImportItemBeanAndClassicAttributes

	{

	static final long serialVersionUID = 3206093459760846163L;
	
	private static int ct=1002;
	
	//String dbURL = "jdbc:db2://CTSC00579472801.cts.com:50000/labdb:user=wasadmin;password=Pass$007;";
	//String dbURL = "jdbc:derby:D:\\WCSLAB\\WCDE_ENT70\\db\\mall";
	
	
	private boolean isSKUBeanAlreadyExisting(DBCollection col,Long itemBeanId){
		  DBObject filterObject=new BasicDBObject();
	         filterObject.put("CATENTRY_ID", itemBeanId);
	         long existingRecordForChildId=col.getCount(filterObject);
	         if(existingRecordForChildId>0){
	        	 return true;
	         }
	         return false;
	}
	
	
	public void insertCatentry(DBCollection col,Long itemBeanId)
    throws javax.naming.NamingException, SQLException
{

 
		Connection conn=null;
		
     try {
    	
         // get a connection from the WebSphere Commerce data source
 		conn=DBUtil.makeDb2Connection();
         System.out.println("After making connection to DB2:"+itemBeanId);
         
         PreparedStatement stmt = null;    
    
    	 
         String query = "SELECT  CAT.CATENTRY_ID,CAT.PARTNUMBER,CAT.MFPARTNUMBER,CAT.FIELD1,CAT.FIELD2, CAT.URL,CAT.LASTUPDATE,CAT.FIELD3,CAT.FIELD4,CAT.STARTDATE,CAT.ENDDATE,CAT.AVAILABILITYDATE,CAT.DISCONTINUEDATE,CAT.UP_MFNAME,CAT.UP_MFPARTNUMBER,CDESC.LANGUAGE_ID,CDESC.NAME,CDESC.SHORTDESCRIPTION,CDESC.LONGDESCRIPTION,CDESC.THUMBNAIL,CDESC.AUXDESCRIPTION1,CDESC.FULLIMAGE,CDESC.AUXDESCRIPTION2,CDESC.XMLDETAIL,CDESC.AVAILABLE,CDESC.AVAILABILITYDATE,CDESC.KEYWORD,CDESC.UP_NAME, CAT.MARKFORDELETE, CAT.BUYABLE ,CDESC.PUBLISHED  FROM CATENTRY CAT,CATENTDESC CDESC WHERE  CAT.catentry_id=CDESC.catentry_id AND CAT.CATENTRY_ID=? WITH UR";
         stmt = conn.prepareStatement(query);    
         stmt.setLong(1, itemBeanId);
        
         ResultSet rs = stmt.executeQuery();
         
       //  stmt.close();
            
         DBObject bson =null;
         ResultSetMetaData rsmd = rs.getMetaData();
         int numberOfColumns = rsmd.getColumnCount();
         
         
        // int ct=1000;
         String temp=null;
         while(rs.next())
         {
        	 //System.out.println("Count is:+ct");
        	 bson= new BasicDBObject();
        	 bson.put("_id", ++MongoImportItemBeanAndClassicAttributes.ct);
        	 for(int k=1;k<=numberOfColumns;++k){
        		 
        		 populateBSONfromResultSet(rs, bson, rsmd, k);
        	 }            
        	 col.insert(bson);
        	// bsonlist.add(bson);
         }
         
      
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
	
	
	MongoClient mongoClient=null;
	mongoClient =DBUtil.getMongoDBClient();
	DB db = mongoClient.getDB( "test" );
	DBCollection collection = db.getCollection("CATENTRY");
	DBObject bson =  new BasicDBObject( "ATTRIBUTES",null);
	
	DBCursor cursor=collection.find(bson);
	
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


private List<Long> getChildCatentriesToInsert() throws UnknownHostException{
	
	MongoClient client=DBUtil.getMongoDBClient();
	List<Long> resultList=new ArrayList<Long>();
	DB db = client.getDB( "test" );	
	BasicDBObject fields = new BasicDBObject();
	fields.put("CHILD_CATENTRY_LIST", 1);
	DBCursor cursor=db.getCollection("CATENTRY").find(null,fields);
	Long catentryId=null;
	String childIds=null;
	String[] itemBeanIds=null;
	String childIdModified=null;
	DBObject result=null;
	while(cursor.hasNext()){
		DBObject object=cursor.next();
		result=(DBObject)object.get("CHILD_CATENTRY_LIST");
		if(result!=null){
			
		
		//childIds=result.toString();
		childIds=(String)result.get("CATENTRY_ID_CHILD");
		//System.out.println("ChildIDs:"+childIds);
		//childIdModified=childIds.replaceAll("\"", "");
		//System.out.println("childIdModified:"+childIds);
		itemBeanIds=childIds.split(",");
		for(String string:itemBeanIds){
			//System.out.println("CatentryID to convert:"+string);
			if(string!=null && !string.isEmpty()){
				resultList.add(Long.valueOf(string));	
			}
			
		}
		}
	}
	return resultList;

	
}
	
	
	 

public static void main(String[] args) {
	MongoImportItemBeanAndClassicAttributes impData=new MongoImportItemBeanAndClassicAttributes();
	
	try {
			long starttime=System.currentTimeMillis();
			
			List<Long> itemBeanIds=impData.getChildCatentriesToInsert();
			System.out.println("Child catentrySize is:"+itemBeanIds.size());
			MongoClient mongoClient=null;
			String table="CATENTRY";
			mongoClient =DBUtil.getMongoDBClient();
	 		System.out.println("After making connection to mongoClient");
	 		  
	         DB db = mongoClient.getDB( "test" );		
	         DBCollection col = db.getCollection(table);
	         
	       
	 		
			for(Long itemBeanId:itemBeanIds){
					System.out.println("itemBeanId:"+itemBeanId);
				  if(!impData.isSKUBeanAlreadyExisting(col, itemBeanId)){
					  impData.insertCatentry(col,itemBeanId);	  
				  }else{
					  System.out.println("SKU:"+itemBeanId+":Already existis in Magento");
				  }
				
			}
			
	
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