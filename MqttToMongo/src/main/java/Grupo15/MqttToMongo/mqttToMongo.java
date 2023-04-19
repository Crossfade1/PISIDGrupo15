package Grupo15.MqttToMongo;

import java.sql.Timestamp;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.StructType;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class mqttToMongo {

    public static void main(String[] args) throws Exception {

        // Spark SQL configuration
        SparkSession spark = SparkSession.builder()
                .appName("MqttToMongoDBExample")
                .master("local")
                .getOrCreate();

        // Create a Kafka source to read data from MQTT
        Dataset<Row> mqttData = spark.readStream()
                .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
                .option("topic", "example_topic")
                .option("clientId", "mqtt_to_mongodb_example")
                .option("brokerUrl", "tcp://localhost:1883")
                .option("cleanSession", "true")
                .load();

        // Define the schema for the MQTT data
        StructType schema = new StructType()
                .add("data", "string")
                .add("timestamp", "timestamp");

        // Convert the MQTT data to a DataFrame with the defined schema
        Dataset<Row> mqttDataFrame = mqttData.selectExpr("CAST(payload AS STRING) as data", "current_timestamp() as timestamp")
                .select("data", "timestamp");

        // Start the MQTT message callback
        mqttDataFrame.writeStream()
                .outputMode("append")
                .option("checkpointLocation", "/path/to/checkpoint/dir")
                .foreach(new ForeachWriter<Row>() {

                    // Define the MongoDB connection settings
                    MongoClient mongoClient = new MongoClient("localhost", 27017);
                    MongoDatabase database = mongoClient.getDatabase("mydb");
                    MongoCollection<Document> collection = database.getCollection("mycollection");

                    @Override
                    public boolean open(long partitionId, long version) {
                        return true;
                    }

                    @Override
                    public void process(Row row) {
                        // Extract the data and timestamp from the row
                        String data = row.getString(0);
                        Timestamp timestamp = row.getTimestamp(1);

                        // Create a MongoDB document
                        Document document = new Document();
                        document.put("data", data);
                        document.put("timestamp", timestamp);

                        // Insert the document into MongoDB
                        collection.insertOne(document);
                    }

                    @Override
                    public void close(Throwable errorOrNull) {
                        mongoClient.close();
                    }
                })
                .start()
                .awaitTermination();
    }
}
