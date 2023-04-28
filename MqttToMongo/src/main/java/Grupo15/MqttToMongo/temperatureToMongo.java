package Grupo15.MqttToMongo;

import java.util.Random;

import org.apache.hadoop.shaded.com.nimbusds.jose.shaded.json.JSONObject;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
//import com.mongodb.MongoTimeoutException;
//import com.mongodb.WriteResult;
import com.mongodb.util.JSON;


public class temperatureToMongo implements MqttCallback{
	
	private DB db;
	private String mongo_Tempcollection;
    static DBCollection mongocol;

	MqttClient mqttclient;
	private String cloud_server = new String();
    private String cloud_topic = new String();
    
    private messageList backup;

	
	public temperatureToMongo(DB database, String mongo_Tempcollection,String cloud_server, String cloud_topic, messageList backup) {
		this.db = database;
		this.mongo_Tempcollection = mongo_Tempcollection;
		this.cloud_server = cloud_server;
		this.cloud_topic = cloud_topic;
		this.backup=backup;
	}
	

	
	//@Override
	//public void run() {
		//accessTempCollection();
	//}
	
	public void accessTempCollection() {
		mongocol = db.getCollection(mongo_Tempcollection);	
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
            System.out.println("Connectado com sucesso! - TemperaturesToMongo");
        } catch (MqttException e) {
            e.printStackTrace();
        }
	}
	
	private String handleMessage(String message) {
		String[] msgArray=message.split(","); //Divide a informação do sensor recebida em campos para analise;
		String result = "";
		result += msgArray[0] + ","; //A data não é verificada
		//Tratar da parte da leitura do sensor
		String[] leituraLegenda = msgArray[1].split(":");
		result += leituraLegenda[0]+":";
		boolean isCorrect = true;
		for (char c : leituraLegenda[1].replace(" ", "").toCharArray()) {
			if(!Character.isDigit(c) && c!='.') {
				isCorrect = false;
			}
		}
		if (!isCorrect) {
			result += '"' + leituraLegenda[1] + '"' + ",";		
		} else {
			result += leituraLegenda[1] + ",";
		}
		//Tratar da parte da identificação do sensor
		String[] sensorLegenda = msgArray[2].split(":");
		result += sensorLegenda[0]+":";
		boolean isCorrectSensor = true;
		for (char c : sensorLegenda[1].replace(" ", "").toCharArray()) {
			if(!Character.isDigit(c) && c!='.') {
				isCorrectSensor = false;
			}
		}
		if (!isCorrectSensor) {
			result += '"' + sensorLegenda[1] + '"' + ",";		
		} else {
			result += sensorLegenda[1] + ",";
		}
		return result;
	}
	
	@Override
    public void messageArrived(String topic, MqttMessage c) throws Exception{
		//JSON.parse() dá erro quando aparece 3@, como evitar?
		String message = handleMessage(c.toString());
        try {	
    		System.out.println("Mensagem recebida: " + message);
            DBObject document_json;
            //System.out.println(c.toString().replace("@", "/"));
            document_json = (DBObject) JSON.parse(message);
            //System.out.println(document_json);
            //mongocol.insert(document_json);
        } catch (Exception e) {
        	System.out.println(e);
            //System.err.println("Servidor down");
        	//System.err.println("Não foi possível escrever a seguinte mensagem no mongo: \n" + message);
        	//forceSend(c.toString());
        	this.messageArrived(topic, c);
            //backup.put(new mensagemMQTT(this.cloud_topic, c.toString()));
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
