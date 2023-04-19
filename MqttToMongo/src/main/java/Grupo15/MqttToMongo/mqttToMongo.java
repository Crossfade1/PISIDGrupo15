package Grupo15.MqttToMongo;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.mongodb.*;
import com.mongodb.util.JSON;

import java.util.*;
import java.util.Vector;
import java.io.File;
import java.io.*;
import javax.swing.*;
import java.awt.*;
import java.awt.event.*;


public class mqttToMongo implements MqttCallback {
	MqttClient mqttclient;
    static MongoClient mongoClient;
    static DB db;
    static DBCollection mongocol;
	static String mongo_user = new String();
	static String mongo_password = new String();
	static String mongo_address = new String();
	static String cloud_server = new String();
    static String cloud_topic = new String();
    static String mongo_host = new String();
    static String mongo_replica = new String();
	static String mongo_database = new String();
    static String mongo_collection = new String();
	static String mongo_authentication = new String();
	//static JTextArea documentLabel = new JTextArea("\n");
    
    private void getproperties() {
    	try {
            Properties p = new Properties();
            p.load(new FileInputStream("cloudToMongo.ini"));
			mongo_address = p.getProperty("mongo_address");
            mongo_user = p.getProperty("mongo_user");
            mongo_password = p.getProperty("mongo_password");						
            mongo_replica = p.getProperty("mongo_replica");
            cloud_server = p.getProperty("cloud_server");	
            //System.out.println(cloud_server);
            cloud_topic = p.getProperty("cloud_topic");
            mongo_host = p.getProperty("mongo_host");
            mongo_database = p.getProperty("mongo_database");
            mongo_authentication = p.getProperty("mongo_authentication");			
            mongo_collection = p.getProperty("mongo_collection");
        } catch (Exception e) {
            System.out.println("Error reading CloudToMongo.ini file " + e);
            JOptionPane.showMessageDialog(null, "The CloudToMongo.inifile wasn't found.", "CloudToMongo", JOptionPane.ERROR_MESSAGE);
        }
    	connectCloud();
    }
	
    public void connectCloud() {
    	System.out.println("Connecting....");
		int i;
        try {
			i = new Random().nextInt(100000);
            mqttclient = new MqttClient(cloud_server, "CloudToMongo_"+String.valueOf(i)+"_"+cloud_topic);
            mqttclient.connect();
            mqttclient.setCallback(this);
            mqttclient.subscribe(cloud_topic);
            System.out.println("Connectado com sucesso!");
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void messageArrived(String topic, MqttMessage c) throws Exception {
        try {	
        		System.out.println("Mensagem recebida: " + c.toString());
        		//System.out.println(c.toString());
                //DBObject document_json;
                //document_json = (DBObject) JSON.parse(c.toString());
                //mongocol.insert(document_json);     	
				//documentLabel.append(c.toString()+"\n");				
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
    }
    
    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }	
	

	
	
	
	public static void main( String[] args )
    {
		new mqttToMongo().getproperties();
    }
}
