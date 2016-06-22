package com.cognizant.mongodb;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.cognizant.mongodb.util.DBUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/**
 * Bean implementation class for Enterprise Bean: MongoImportProductManagementData
 */
public class MongoUpdateCatentryWithChildBeans

	{
	
	Logger logger=Logger.getLogger(MongoUpdateCatentryWithChildBeans.class.getName());

	static final long serialVersionUID = 3206093459760846163L;
	
	//String dbURL = "jdbc:db2://CTSC00579472801.cts.com:50000/labdb:user=wasadmin;password=Pass$007;";
	//String dbURL = "jdbc:derby:D:\\WCSLAB\\WCDE_ENT70\\db\\mall";
	
	static final String DB2_QUERY="SELECT CATENTRY_ID_CHILD FROM CATENTREL WHERE CATENTRY_ID_PARENT=? WITH UR";
	static final String COMMA=",";
	

	public List<Long> getParentCantryIdsFromMongo() throws UnknownHostException{
		MongoClient client=DBUtil.getMongoDBClient();
		List<Long> resultList=new ArrayList<Long>();
		DB db = client.getDB( "test" );	
		BasicDBObject fields = new BasicDBObject();
		fields.put("CATENTRY_ID", 1);
		DBCursor cursor=db.getCollection("CATENTRY").find(null,fields);
		Long catentryId=null;
		while(cursor.hasNext()){
			DBObject object=cursor.next();
			catentryId=(Long)object.get("CATENTRY_ID");
			
			resultList.add(catentryId);
			
		}
	
		
		return resultList;
	}

	public void updateChildIdInMongo(List<Long> parentCatentryIds) throws SQLException, UnknownHostException{
		logger.log(Level.FINE, "Entering updateChildIdInMongo");
		List<Long> childCatentryIds=null;
		Connection connection=DBUtil.makeDb2Connection();
		DBObject catentryObject=null;
		
		MongoClient client=DBUtil.getMongoDBClient();
		DB db = client.getDB( "test" );	
		BasicDBObject fields = null;
		DBCollection collection=db.getCollection("CATENTRY");
		
		for(Long parentCatentryId:parentCatentryIds){
			logger.log(Level.FINE, "Getting child catentries for:"+parentCatentryId);
			PreparedStatement preparedStatement=connection.prepareStatement(DB2_QUERY);
			ResultSet resultSet=getItemBeans(parentCatentryId,preparedStatement);
			fields = new BasicDBObject();
			fields.put("CATENTRY_ID", parentCatentryId);
			catentryObject=collection.findOne(fields);
			//catentryObject=getMongoParentCatentryObject(parentCatentryId);
			 BasicDBObject childCatentryListBSON = new BasicDBObject();
			 int start=0;
			 StringBuffer childIds=new StringBuffer();
			 while (resultSet.next()){
				 if(start!=0){
					 childIds.append(COMMA);
				 }
				 childIds.append(resultSet.getLong(1));
				 start=1;         	
		       }
			 childCatentryListBSON.put("CATENTRY_ID_CHILD",childIds.toString());
			 catentryObject.put("CHILD_CATENTRY_LIST", childCatentryListBSON);
			 
			 collection.save(catentryObject);
			 
			 try {
				resultSet.close();
				 preparedStatement.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.log(Level.SEVERE, "Exception while closing connection after updating parent catenryId:"+parentCatentryId, e);
				System.out.println("Exception while updating parent catentryId:");
				e.printStackTrace();
			}
		}
		
		logger.log(Level.FINE, "Exiting updateChildIdInMongo");
		
		
	}
	
	private DBObject getMongoParentCatentryObject(Long catentryId) throws UnknownHostException{
		MongoClient client=DBUtil.getMongoDBClient();
		DB db = client.getDB( "test" );	
		BasicDBObject fields = new BasicDBObject();
		fields.put("CATENTRY_ID", catentryId);
		DBObject catentryObject=db.getCollection("CATENTRY").findOne(fields);
		return catentryObject;
		
	}


	private ResultSet getItemBeans(Long parentCatentryId,PreparedStatement preparedStatement) throws SQLException{
		logger.log(Level.FINE, "Entering getItemBeans");
		
		preparedStatement.setLong(1, parentCatentryId);
		ResultSet resultSet=preparedStatement.executeQuery();
		return resultSet;
	}

	
	
	 

public static void main(String[] args) {
	MongoUpdateCatentryWithChildBeans impData=new MongoUpdateCatentryWithChildBeans();
	
	try {
			long starttime=System.currentTimeMillis();
			List<Long> parentIds=impData.getParentCantryIdsFromMongo();
			System.out.println("Length of catentries is:"+parentIds.size());
			long endtime1=System.currentTimeMillis();
			System.out.println("TIme taken is:"+(endtime1-starttime)+"Millis");
			impData.updateChildIdInMongo(parentIds);
	
			//impData.updateCatentriesWithAttributes();
			long endtime=System.currentTimeMillis();
			System.out.println("Time taken to insert child catentries:"+(endtime-endtime1)+"millis");
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	/* catch (NamingException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	*/ 
}

}