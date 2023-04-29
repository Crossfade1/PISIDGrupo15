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
    
    private int number =0;

	
	public temperatureToMongo(DB database, String mongo_Tempcollection,String cloud_server, String cloud_topic) {
		this.db = database;
		this.mongo_Tempcollection = mongo_Tempcollection;
		this.cloud_server = cloud_server;
		this.cloud_topic = cloud_topic;
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
		//result += msgArray[0] + ","; //A data não é verificada
		result+=checkDate(msgArray[0]);
		
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
			System.err.println("DADO ENVIADO ERRADO (LEITURA) => " + leituraLegenda[1]);
		} else {
			result += leituraLegenda[1] + ",";
		}
		
		//Tratar da parte da identificação do sensor
		String[] sensorLegenda = msgArray[2].split(":");
		result += sensorLegenda[0]+":";
		boolean isCorrectSensor = true;
		for (char c : sensorLegenda[1].replace(" ", "").toCharArray()) {
			if(!Character.isDigit(c) && c!='.' && c!='}') {
				isCorrectSensor = false;
			}
		}
		if (!isCorrectSensor) {
			result += '"' + sensorLegenda[1] + '"' + ",";
			System.err.println("DADO ENVIADO ERRADO (SENSOR) => " + sensorLegenda[1]);
		} else {
			result += sensorLegenda[1] + ",";
		}
		return result;
	}
	
	private String checkDate(String dateMessage) { //Retornar string tratada
		String legend = dateMessage.split(":", 2)[0]+":";
		String date = dateMessage.split(":", 2)[1];
		legend+= String.valueOf('"');
		boolean removeFirstSpace = false;
		for(char c : date.toCharArray()) {
			if (Character.isDigit(c) || c=='-' || c==':' || c=='.' || (c==' ' && removeFirstSpace))
				legend+=c;
			
			if(c==' ' && !removeFirstSpace)
				removeFirstSpace=true;
		}
		legend+= String.valueOf('"') + ", ";
		return legend;
	}
	
	@Override
    public void messageArrived(String topic, MqttMessage c) throws Exception{
		String message = handleMessage(c.toString());
        try {	
    		System.out.println("Mensagem recebida: " + message);
            DBObject document_json;
            document_json = (DBObject) JSON.parse(message);
            mongocol.insert(document_json);
            this.number++;
            System.out.println(number);
        } catch (Exception e) {
        	System.err.println("Não foi possível escrever a seguinte mensagem no mongo: \n" + message);
        	this.messageArrived(topic, c);
        }
    }

	@Override
	public void connectionLost(Throwable cause) {
		System.err.println("CONEXÃO PERDIDA COM O MQTT BROKER");
		
	}


	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		// TODO Auto-generated method stub
		
	}
	
}
