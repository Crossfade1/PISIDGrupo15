package Grupo15.MqttToMongo;

import java.util.Random;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoTimeoutException;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

public class movesToMongo implements MqttCallback{

	private DB db;
	private String mongo_Movescollection;
    static DBCollection mongocol;

	MqttClient mqttclient;
	private String cloud_server = new String();
    private String cloud_topic = new String();
    
    private messageList backup;
	
	public movesToMongo(DB database, String mongo_Movescollection,String cloud_server, String cloud_topic, messageList backup) {
		this.db = database;
		this.mongo_Movescollection = mongo_Movescollection;
		this.cloud_server = cloud_server;
		this.cloud_topic = cloud_topic;
		this.backup=backup;
	}
	

	
	//@Override
	//public void run() {
		//accessTempCollection();
	//}
	
	public void accessMoveCollection() {
		mongocol = db.getCollection(mongo_Movescollection);	
		System.out.println("Collection added");
	    connectCloud();
	}
	
	private void connectCloud() {
		System.out.println("Connecting....");
		int i;
        try {
			i = new Random().nextInt(100000);
            mqttclient = new MqttClient(cloud_server, "CloudToMongo_"+String.valueOf(i)+"_"+cloud_topic);
            mqttclient.connect();
            mqttclient.setCallback(this);
            mqttclient.subscribe(cloud_topic);
            System.out.println("Connectado com sucesso! - MovesToMongo");
        } catch (MqttException e) {
            e.printStackTrace();
        }
	}
	
	@Override
    public void messageArrived(String topic, MqttMessage c) throws Exception{
       try {	
    		System.out.println("Mensagem recebida: " + c.toString());
            DBObject document_json;
            document_json = (DBObject) JSON.parse(c.toString());
            WriteResult result = mongocol.insert(document_json);
            
            if (result.wasAcknowledged()) {
                System.out.println("Document inserted successfully");
            } else {
            	System.err.println("Servidor down");
                backup.put(new mensagemMQTT(this.cloud_topic, c.toString()));
            }   	
        } catch (MongoTimeoutException e) {
            System.out.println(e);
           // System.err.println("Servidor down");
           // backup.put(new mensagemMQTT(this.cloud_topic, c.toString()));
        }
    }

	@Override
	public void connectionLost(Throwable cause) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		// TODO Auto-generated method stub
		
	}
	
}
