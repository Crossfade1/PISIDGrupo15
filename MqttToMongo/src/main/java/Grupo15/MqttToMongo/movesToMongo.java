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
    
    private int number =0;
	
	public movesToMongo(DB database, String mongo_Movescollection,String cloud_server, String cloud_topic) {
		this.db = database;
		this.mongo_Movescollection = mongo_Movescollection;
		this.cloud_server = cloud_server;
		this.cloud_topic = cloud_topic;
	}
	
	
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
	
	
	private String handleMessage(String message) {
		String[] msgArray = cleanMessage(message).split(","); //Divide a informação do sensor recebida em campos para analise;
		String result = "";
		result+=checkDate(msgArray[0]);
		result+=checkFrom(msgArray[1]) + ",";
		result+=checkFrom(msgArray[2]);
		return result;
	}
	
	private String cleanMessage(String message) {
		String result = "";
		if(message.startsWith("movimentação ratos:")) {
			result = message.split(" ", 3)[2];
			System.err.println("ERRO (FORMATO MENSAGEM) => " + result);
		} else {
			result = message;
		}
		return result;
	}
	
	private String checkDate(String dateMessage) { //Retornar string tratada
		String legend = dateMessage.split(":", 2)[0]+":";
		String date = dateMessage.split(":", 2)[1];
		legend+= String.valueOf('"');
		boolean removeFirstSpace = true;
		for(char c : date.toCharArray()) {
			if (Character.isDigit(c) || c=='-' || c==':' || c=='.' || (c==' ' && removeFirstSpace))
				legend+=c;
			
			if(c==' ' && !removeFirstSpace)
				removeFirstSpace=true;
		}
		legend+= String.valueOf('"') + ", ";
		return legend;
	}
	
	private String checkFrom(String field) {
		String result = "";
		//Tratar da parte da identificação do sensor
		String[] sensorLegenda = field.split(":");
		if(sensorLegenda[0].replace(" ", "").equals("from")) {
			result += " SalaEntrada:";
			System.err.println("ERRO (ORIGEM) => " + sensorLegenda[0]);
		}
		else if(sensorLegenda[0].replace(" ", "").equals("to")) {
			result += " SalaSaida:";
			System.err.println("ERRO (DESTINO) => " + sensorLegenda[0]);
		} else {
			result += sensorLegenda[0]+":";
		}
		boolean isCorrectSensor = true;
		for (char c : sensorLegenda[1].replace(" ", "").toCharArray()) {
			if(!Character.isDigit(c) && c!='.' && c!='}') {
				isCorrectSensor = false;
			}
		}
		if (!isCorrectSensor) {
			result += '"' + sensorLegenda[1] + '"';
			System.err.println("DADO ENVIADO ERRADO (" + sensorLegenda[0] + ") => " + sensorLegenda[1]);
		} else {
			result += sensorLegenda[1];
		}
		return result;
	}
	
	@Override
    public void messageArrived(String topic, MqttMessage c) throws Exception{
		//System.out.println("MENSAGEM POR TRATAR => " + c.toString());
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
		// TODO Auto-generated method stub
		
	}


	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		// TODO Auto-generated method stub
		
	}
	
}
