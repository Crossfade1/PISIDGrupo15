package Grupo15.MqttToMongo;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class backUp extends Thread{
	
	private DB db;
	private String mongo_Tempcollection = new String();
    private String mongo_movecollection = new String();
    static DBCollection mongoTempCollection;
    static DBCollection mongoMoveCollection;
    
    private messageList backup;
	
	public backUp(DB database, String tempCollection, String moveCollection, messageList backup) {
		this.mongo_Tempcollection= tempCollection;
		this.mongo_movecollection = moveCollection;
		this.backup=backup;
		this.db = database;
	}
	
	public void accessMoveCollection() {
		mongoTempCollection = db.getCollection(mongo_Tempcollection);	
		System.out.println("Temp Collection added to backup Thread");
		mongoMoveCollection = db.getCollection(mongo_movecollection);	
		System.out.println("Move Collection added to backup Thread");
	}
	
	@Override
	public void run() {
		accessMoveCollection();
		while(!this.isInterrupted()) {
			try {
				mensagemMQTT currentMessage = backup.get();
				DBObject document_json;
	            document_json = (DBObject)JSON.parse(currentMessage.getMensagem()); 
				if(currentMessage.getTopic().equals("pisid_mazetemp")) {
					mongoTempCollection.insert(document_json);
				} else {
					mongoMoveCollection.insert(document_json);
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
}
