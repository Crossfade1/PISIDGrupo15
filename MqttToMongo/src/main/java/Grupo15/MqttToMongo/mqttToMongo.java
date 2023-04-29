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
import java.util.List;
import java.io.File;
import java.io.*;
import javax.swing.*;
import java.awt.*;
import java.awt.event.*;


public class mqttToMongo {
	MqttClient mqttclient;
    static MongoClient mongoClient;
    static DB db;
    //static DBCollection mongocol;
	static String mongo_user = new String();
	static String mongo_password = new String();
	static String mongo_address = new String();
	static String cloud_server = new String();
    static String cloud_Temptopic = new String();
    static String cloud_Movetopic = new String();
    static String mongo_host = new String();
    static String mongo_replica = new String();
    //Mongo DB and Collections
	static String mongo_database = new String();
    static String mongo_Tempcollection = new String();
    static String mongo_movecollection = new String();
    static String mongo_backupcollection = new String();
	static String mongo_authentication = new String();
	//static JTextArea documentLabel = new JTextArea("\n");
	
    private void getproperties() {
    	try {
            Properties p = new Properties();
            p.load(new FileInputStream("cloudToMongo.ini"));
            //Mqtt Properties
            cloud_server = p.getProperty("cloud_server");	
            cloud_Temptopic = p.getProperty("cloud_Temptopic");
            cloud_Movetopic = p.getProperty("cloud_Movetopic");
            //Mongo
            mongo_address = p.getProperty("mongo_address");
            mongo_user = p.getProperty("mongo_user");
            mongo_password = p.getProperty("mongo_password");						
            mongo_replica = p.getProperty("mongo_replica");
            mongo_host = p.getProperty("mongo_host");
            mongo_database = p.getProperty("mongo_database");
            mongo_authentication = p.getProperty("mongo_authentication");			
            mongo_Tempcollection = p.getProperty("mongo_Tempcollection");
            mongo_movecollection = p.getProperty("mongo_movecollection");
            mongo_backupcollection = p.getProperty("mongo_backupcollection");
        } catch (Exception e) {
            System.out.println("Error reading CloudToMongo.ini file " + e);
            JOptionPane.showMessageDialog(null, "The CloudToMongo.inifile wasn't found.", "CloudToMongo", JOptionPane.ERROR_MESSAGE);
        }
    	connectMongo();
    }
	
    
    public void connectMongo() {
		String mongoURI = new String();
		mongoURI = "mongodb://";		
		if (mongo_authentication.equals("true")) mongoURI = mongoURI + mongo_user + ":" + mongo_password + "@";		
		mongoURI = mongoURI + mongo_address;		
		if (!mongo_replica.equals("false")) 
			if (mongo_authentication.equals("true")) mongoURI = mongoURI + "/?replicaSet=" + mongo_replica+"&authSource=admin";
			else mongoURI = mongoURI + "/?replicaSet=" + mongo_replica;		
		else
			if (mongo_authentication.equals("true")) mongoURI = mongoURI  + "/?authSource=admin";			
		MongoClient mongoClient = new MongoClient(new MongoClientURI(mongoURI));						
		db = mongoClient.getDB(mongo_database);
		startTempThread();
		startMovesThread();
    }
    
    private void startTempThread() {
		temperatureToMongo ttM = new temperatureToMongo(db, mongo_Tempcollection, cloud_server, cloud_Temptopic);
		ttM.accessTempCollection();
    }
    
    private void startMovesThread() {
		movesToMongo mT = new movesToMongo(db, mongo_movecollection, cloud_server, cloud_Movetopic);
		mT.accessMoveCollection();
    }
    
	

	
	
	
	public static void main( String[] args )
    {
		new mqttToMongo().getproperties();
    }
}
