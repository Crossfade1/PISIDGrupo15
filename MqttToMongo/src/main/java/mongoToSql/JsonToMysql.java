package mongoToSql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.bson.Document;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class JsonToMysql {
	private static final String DB_URL = "jdbc:mysql://localhost:3306/experienciaratos";
	private static final String DB_USER = "adminBD";
	private static final String DB_PASSWORD = "1234";
	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
	private final static MongoCollection<Document> BackupCollectionTemp = MongoClients
			.create("mongodb://localhost:27017").getDatabase("Backup").getCollection("temperatura");
	private final static MongoCollection<Document> BackupCollectionMov = MongoClients
			.create("mongodb://localhost:27017").getDatabase("Backup").getCollection("movimentacaoRatos");
	private static List<Double> Outlier;

	public static void AddDocumentBackupTemp(Document backup) {
		BackupCollectionTemp.insertOne(backup);
	}

	public static void AddDocumentBackupMov(Document backup) {
		BackupCollectionMov.insertOne(backup);
	}

	public static double[] ArrayToVector(List<Double> outlier) {
		double[] outlierDouble = new double[10];
		for (int i = 0; i < 10; i++) {
			outlierDouble[i] = Outlier.get(i);
		}
		return outlierDouble;
	}

	private static boolean Outliers(double[] vector) {
		double lastValue = vector[vector.length - 1];
		double sum = 0.0;

		for (int i = 0; i < vector.length - 1; i++) {
			sum += vector[i];
		}

		double average = sum / (vector.length - 1);
		double difference = Math.abs(average - lastValue);
		double percentage = (difference / lastValue) * 100;

		return percentage <= 20.0;
	}

	public static void StartArray() {
		Outlier = new ArrayList<>();
	}

	public static void DecreaseOutlier(double d) {
		Outlier.set(0, Outlier.get(1));
		Outlier.set(1, Outlier.get(2));
		Outlier.set(2, Outlier.get(3));
		Outlier.set(3, Outlier.get(4));
		Outlier.set(4, Outlier.get(5));
		Outlier.set(5, Outlier.get(6));
		Outlier.set(6, Outlier.get(7));
		Outlier.set(7, Outlier.get(8));
		Outlier.set(8, Outlier.get(9));
		Outlier.set(9, d);
	}

	public static void RemoveMongoDBTemp() {
		BackupCollectionTemp.deleteMany(new Document());
	}

	public static void RemoveMongoDBMov() {
		BackupCollectionMov.deleteMany(new Document());
	}

	public static void insertDataFromBackup(String json, Boolean topic, Date currenttime, Document doc)
			throws ParseException { // true for temperature false for movement

		StartArray();
		ObjectMapper mapper = new ObjectMapper();
		JsonNode rootNode = null;
		try {
			rootNode = mapper.readTree(json);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			return;
		}

		try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {

			String sql = new String();
			PreparedStatement statement;

			if (topic == true) {
				sql = "CALL `insertTemperatureData`(?, ?, ?)";

				JsonNodeType horaNode = rootNode.get("Hora").getNodeType();
				JsonNodeType leituraNode = rootNode.get("Leitura").getNodeType();
				JsonNodeType sensorNode = rootNode.get("Sensor").getNodeType();

				if (!horaNode.equals(JsonNodeType.STRING) || !leituraNode.equals(JsonNodeType.NUMBER)
						|| !sensorNode.equals(JsonNodeType.NUMBER)) {

					// verificar se os datatypes sao os certos
					sql = "CALL `insertAlert`(?,?,?,?,?,?)";
					statement = conn.prepareStatement(sql);
					statement.setObject(1, rootNode.get("Hora").asText());
					statement.setObject(2, 0);
					statement.setObject(3, rootNode.get("Sensor").asText());
					statement.setObject(4, rootNode.get("Leitura").asText());
					statement.setString(5, "Tipo de Dados invalido");
					statement.setString(6, "Verificar se os tipos de dados recebidos sao coerentes com o esperado");

					statement.executeUpdate();
					System.out.println("Aviso Escrito");
					return;
				}
				
				Timestamp Hora;
				double Leitura = rootNode.get("Leitura").asDouble();
				int Sensor = rootNode.get("Sensor").asInt();

				try {
					Hora = Timestamp.valueOf(rootNode.get("Hora").asText());
				} catch (IllegalArgumentException e) {
					// verificar se a data esta em formato certo
					sql = "CALL `insertAlert`(?,?,?,?,?,?)";
					statement = conn.prepareStatement(sql);
					statement.setTimestamp(1, null);
					statement.setInt(2, 0);
					statement.setInt(3, Sensor);
					statement.setDouble(4, Leitura);
					statement.setString(5, "Data Invalida: " + rootNode.get("Hora").asText());
					statement.setString(6, "Formato de data nao e o esperado");

					statement.executeUpdate();
					System.out.println("Aviso Escrito");
					return;
				}
				
				String dateString = Hora.toString();
				Date date;

				try {
					date = dateFormat.parse(dateString);
				} catch (NumberFormatException e) {
					if (dateString.equals("")) {
						sql = "CALL `insertAlert`(?,?,?,?,?,?)";
						statement = conn.prepareStatement(sql);
						statement.setTimestamp(1, null);
						statement.setInt(2, 0);
						statement.setInt(3, Sensor);
						statement.setDouble(4, Leitura);
						statement.setString(5, "Data Invalida:" + Hora);
						statement.setString(6, "Data anterior ao comeco da experiencia");
						statement.executeUpdate();
						System.out.println("Aviso Escrito");
						return;
					}
					e.printStackTrace();
					return;
				}
				
				if (currenttime.after(date)) { // verificar se a data nao e antes da experiencia

					sql = "CALL `insertAlert`(?,?,?,?,?,?)";
					statement = conn.prepareStatement(sql);
					statement.setTimestamp(1, Hora);
					statement.setInt(2, 0);
					statement.setInt(3, Sensor);
					statement.setDouble(4, Leitura);
					statement.setString(5, "Data Invalida");
					statement.setString(6, "Data anterior ao comeco da experiencia");
					statement.executeUpdate();
					System.out.println("Aviso Escrito");
					return;

				}
				statement = conn.prepareStatement(sql);
				statement.setString(1, dateString);
				statement.setDouble(2, Leitura);
				statement.setInt(3, Sensor);

				// "Hora": "2023-04-29 12:38:17.759236", "Leitura": 14, "Sensor": 1} formato

				statement.executeUpdate();
				RemoveMongoDBTemp();

			} else {

				sql = "CALL `inserMovementData`(?, ?, ?)";

				JsonNodeType horaNode;

				try {
					horaNode = rootNode.get("Hora").getNodeType();
				} catch (NullPointerException e) {
					horaNode = rootNode.get("hora").getNodeType();
				}

				JsonNodeType salaentradaNode = rootNode.get("SalaEntrada").getNodeType();
				JsonNodeType salasaidaNode = rootNode.get("SalaSaida").getNodeType();

				if (!horaNode.equals(JsonNodeType.STRING) || !salaentradaNode.equals(JsonNodeType.NUMBER)
						|| !salasaidaNode.equals(JsonNodeType.NUMBER)) {
					// verificar se os datatypes sao os certos

					sql = "CALL `insertAlert`(?,?,?,?,?,?)";
					statement = conn.prepareStatement(sql);
					statement.setObject(0, rootNode.get("Hora").asText());
					statement.setObject(1, rootNode.get("SalaEntrada").asText());
					statement.setObject(2, 0);
					statement.setObject(3, 0);
					statement.setString(4, "Tipo de Dados invalido");
					statement.setString(5, "Verificar se os tipos de dados recebidos sao coerentes com o esperado");

					statement.executeUpdate();
					System.out.println("Aviso Escrito");
					return;
				}

				Timestamp Hora;
				int SalaEntrada = rootNode.get("SalaEntrada").asInt();
				int SalaSaida = rootNode.get("SalaSaida").asInt();

				try {
					Hora = Timestamp.valueOf(rootNode.get("Hora").asText());
				} catch (NullPointerException e) {
					Hora = Timestamp.valueOf(rootNode.get("hour").asText());
				} catch (IllegalArgumentException e) {
					// verificar se a data esta em formato certo
					sql = "CALL `insertAlert`(?,?,?,?,?,?)";
					statement = conn.prepareStatement(sql);
					statement.setTimestamp(1, null);
					statement.setInt(2, SalaEntrada);
					statement.setInt(3, 0);
					statement.setInt(4, 0);

					try {
						statement.setString(5, "Data Invalida: " + rootNode.get("Hora").asText());
					} catch (NullPointerException e2) {
						statement.setString(5, "Data Invalida: " + rootNode.get("hour").asText());
					}
					statement.setString(6, "Formato de data nao e o esperado");

					statement.executeUpdate();
					System.out.println("Aviso Escrito");
					return;
				}
				
				String dateString = Hora.toString();
				Date date;

				try {
					System.out.println(dateString);
					date = dateFormat.parse(dateString);
				} catch (NumberFormatException e) {

					sql = "CALL `insertAlert`(?,?,?,?,?,?)";
					statement = conn.prepareStatement(sql);
					statement.setTimestamp(1, null);
					statement.setInt(2, SalaEntrada);
					statement.setInt(3, 0);
					statement.setInt(4, 0);
					statement.setString(5, "Data Invalida:" + dateString);
					statement.setString(6, "Data em formato errado");
					statement.executeUpdate();
					System.out.println("Aviso Escrito");
					System.out.println("NO CASO DE ESTAR MAL FOI ISTO " + dateString);
					return;

				}


				if (currenttime.after(date)) { // verificar se a data nao e antes da experiencia

					sql = "CALL `insertAlert`(?,?,?,?,?,?)";
					statement = conn.prepareStatement(sql);
					statement.setTimestamp(1, Hora);
					statement.setInt(2, SalaEntrada);
					statement.setInt(3, 0);
					statement.setInt(4, 0);
					statement.setString(5, "Data Invalida");
					statement.setString(6, "Data anterior ao comeco da experiencia");

					statement.executeUpdate();

					System.out.println("Aviso Escrito");

					return;

				}

				// outliers aqui
				statement = conn.prepareStatement(sql);
				statement.setTimestamp(1, Hora);
				statement.setInt(2, SalaEntrada);
				statement.setInt(3, SalaSaida);
				// "Hora": "2023-05-03 18:08:29.817465", "SalaEntrada": 3, "SalaSaida": 2}
				// formato
				statement.executeUpdate();
				RemoveMongoDBMov();
			}

			System.out.println("Data inserted successfully from Backup.");

		} catch (CommunicationsException e) {
			System.out.println("SQL down");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void insertDataFromJson(String json, Boolean topic, Date currenttime, Document doc)
			throws ParseException { // true for temperature false for movement

		StartArray();
		ObjectMapper mapper = new ObjectMapper();
		JsonNode rootNode = null;
		// SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
		try {
			rootNode = mapper.readTree(json);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			return;
		}

		try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {

			String sql = new String();
			PreparedStatement statement;

			if (topic == true) {
				if (Outlier.size() == 10) {
					sql = "CALL `insertTemperatureData`(?, ?, ?)";

					JsonNodeType horaNode = rootNode.get("Hora").getNodeType();
					JsonNodeType leituraNode = rootNode.get("Leitura").getNodeType();
					JsonNodeType sensorNode = rootNode.get("Sensor").getNodeType();

					if (!horaNode.equals(JsonNodeType.STRING) || !leituraNode.equals(JsonNodeType.NUMBER)
							|| !sensorNode.equals(JsonNodeType.NUMBER)) {

						// verificar se os datatypes sao os certos
						sql = "CALL `insertAlert`(?,?,?,?,?,?)";
						statement = conn.prepareStatement(sql);
						statement.setObject(1, rootNode.get("Hora").asText());
						statement.setObject(2, 0);
						statement.setObject(3, rootNode.get("Sensor").asText());
						statement.setObject(4, rootNode.get("Leitura").asText());
						statement.setString(5, "Tipo de Dados invalido");
						statement.setString(6, "Verificar se os tipos de dados recebidos sao coerentes com o esperado");

						statement.executeUpdate();
						System.out.println("Aviso Escrito");
						return;
					}

					Timestamp Hora;
					double Leitura = rootNode.get("Leitura").asDouble();
					int Sensor = rootNode.get("Sensor").asInt();

					try {
						Hora = Timestamp.valueOf(rootNode.get("Hora").asText());
					} catch (IllegalArgumentException e) {
						// verificar se a data esta em formato certo
						sql = "CALL `insertAlert`(?,?,?,?,?,?)";
						statement = conn.prepareStatement(sql);
						statement.setTimestamp(1, null);
						statement.setInt(2, 0);
						statement.setInt(3, Sensor);
						statement.setDouble(4, Leitura);
						statement.setString(5, "Data Invalida: " + rootNode.get("Hora").asText());
						statement.setString(6, "Formato de data nao e o esperado");

						statement.executeUpdate();
						System.out.println("Aviso Escrito");
						return;
					}

					String dateString = Hora.toString();
					Date date;

					try {
						date = dateFormat.parse(dateString);
					} catch (NumberFormatException e) {
						if (dateString.equals("")) {
							sql = "CALL `insertAlert`(?,?,?,?,?,?)";
							statement = conn.prepareStatement(sql);
							statement.setTimestamp(1, null);
							statement.setInt(2, 0);
							statement.setInt(3, Sensor);
							statement.setDouble(4, Leitura);
							statement.setString(5, "Data Invalida:" + Hora);
							statement.setString(6, "Data anterior ao comeco da experiencia");
							statement.executeUpdate();
							System.out.println("Aviso Escrito");
							return;
						}
						e.printStackTrace();
						return;
					}

					DecreaseOutlier(rootNode.get("Leitura").asDouble());

					if (currenttime.after(date)) { // verificar se a data nao e antes da experiencia

						sql = "CALL `insertAlert`(?,?,?,?,?,?)";
						statement = conn.prepareStatement(sql);
						statement.setTimestamp(1, Hora);
						statement.setInt(2, 0);
						statement.setInt(3, Sensor);
						statement.setDouble(4, Leitura);
						statement.setString(5, "Data Invalida");
						statement.setString(6, "Data anterior ao comeco da experiencia");
						statement.executeUpdate();
						System.out.println("Aviso Escrito");
						return;

					}
					if (Outliers(ArrayToVector(Outlier))) {
						sql = "CALL `insertAlert`(?,?,?,?,?,?)";
						statement = conn.prepareStatement(sql);
						statement.setTimestamp(1, Hora);
						statement.setInt(2, 0);
						statement.setInt(3, rootNode.get("Sensor").asInt());
						statement.setDouble(4, rootNode.get("Leitura").asDouble());
						statement.setString(5, "Outlier");
						statement.setString(6, "Leitura fora dos parametros");

						statement.executeUpdate();
						System.out.println("Aviso Escrito");
						return;
					}
					statement = conn.prepareStatement(sql);
					statement.setString(1, dateString);
					statement.setDouble(2, Leitura);
					statement.setInt(3, Sensor);

					// "Hora": "2023-04-29 12:38:17.759236", "Leitura": 14, "Sensor": 1} formato

					statement.executeUpdate();

				} else if (Outlier.size() < 10) {
					sql = "CALL `insertTemperatureData`(?, ?, ?)";

					JsonNodeType horaNode = rootNode.get("Hora").getNodeType();
					JsonNodeType leituraNode = rootNode.get("Leitura").getNodeType();
					JsonNodeType sensorNode = rootNode.get("Sensor").getNodeType();

					if (!horaNode.equals(JsonNodeType.STRING) || !leituraNode.equals(JsonNodeType.NUMBER)
							|| !sensorNode.equals(JsonNodeType.NUMBER)) {

						// verificar se os datatypes sao os certos
						sql = "CALL `insertAlert`(?,?,?,?,?,?)";
						statement = conn.prepareStatement(sql);
						statement.setObject(1, rootNode.get("Hora").asText());
						statement.setObject(2, 0);
						statement.setObject(3, rootNode.get("Sensor").asText());
						statement.setObject(4, rootNode.get("Leitura").asInt());
						statement.setString(5, "Tipo de Dados invalido");
						statement.setString(6, "Verificar se os tipos de dados recebidos sao coerentes com o esperado");

						statement.executeUpdate();
						System.out.println("Aviso Escrito");
						return;
					}

					Timestamp Hora;
					double Leitura = rootNode.get("Leitura").asDouble();
					int Sensor = rootNode.get("Sensor").asInt();

					try {
						Hora = Timestamp.valueOf(rootNode.get("Hora").asText());
					} catch (IllegalArgumentException e) {
						// verificar se a data esta em formato certo
						sql = "CALL `insertAlert`(?,?,?,?,?,?)";
						statement = conn.prepareStatement(sql);
						statement.setTimestamp(1, null);
						statement.setInt(2, 0);
						statement.setInt(3, Sensor);
						statement.setDouble(4, Leitura);
						statement.setString(5, "Data Invalida: " + rootNode.get("Hora").asText());
						statement.setString(6, "Formato de data nao e o esperado");

						statement.executeUpdate();
						System.out.println("Aviso Escrito");
						return;
					}

					String dateString = Hora.toString();
					Date date;

					try {
						date = dateFormat.parse(dateString);
					} catch (NumberFormatException e) {

						sql = "CALL `insertAlert`(?,?,?,?,?,?)";
						statement = conn.prepareStatement(sql);
						statement.setTimestamp(1, null);
						statement.setInt(2, 0);
						statement.setInt(3, Sensor);
						statement.setDouble(4, Leitura);
						statement.setString(5, "Data Invalida:" + dateString);
						statement.setString(6, "Formato de data errado");
						statement.executeUpdate();
						System.out.println("Aviso Escrito");
						System.out.println("NO CASO DE ESTAR MAL FOI ISTO " + dateString);
						return;

					}

					if (currenttime.after(date)) { // verificar se a data nao e antes da experiencia
						System.out.println();
						sql = "CALL `insertAlert`(?,?,?,?,?,?)";
						statement = conn.prepareStatement(sql);
						statement.setTimestamp(1, Hora);
						statement.setInt(2, 0);
						statement.setInt(3, Sensor);
						statement.setDouble(4, Leitura);
						statement.setString(5, "Data Invalida");
						statement.setString(6, "Data anterior ao comeco da experiencia");
						statement.executeUpdate();
						System.out.println("Aviso Escrito");
						return;

					}
					statement = conn.prepareStatement(sql);
					statement.setTimestamp(1, Hora);
					statement.setDouble(2, Leitura);
					statement.setInt(3, Sensor);

					// "Hora": "2023-04-29 12:38:17.759236", "Leitura": 14, "Sensor": 1} formato

					statement.executeUpdate();
					Outlier.add(Leitura);
				}
			} else {

				sql = "CALL `inserMovementData`(?, ?, ?)";

				JsonNodeType horaNode;

				try {
					horaNode = rootNode.get("Hora").getNodeType();
				} catch (NullPointerException e) {
					horaNode = rootNode.get("hora").getNodeType();
				}

				JsonNodeType salaentradaNode = rootNode.get("SalaEntrada").getNodeType();
				JsonNodeType salasaidaNode = rootNode.get("SalaSaida").getNodeType();

				if (!horaNode.equals(JsonNodeType.STRING) || !salaentradaNode.equals(JsonNodeType.NUMBER)
						|| !salasaidaNode.equals(JsonNodeType.NUMBER)) {
					// verificar se os datatypes sao os certos

					sql = "CALL `insertAlert`(?,?,?,?,?,?)";
					statement = conn.prepareStatement(sql);
					statement.setObject(0, rootNode.get("Hora").asText());
					statement.setObject(1, rootNode.get("SalaEntrada").asText());
					statement.setObject(2, 0);
					statement.setObject(3, 0);
					statement.setString(4, "Tipo de Dados invalido");
					statement.setString(5, "Verificar se os tipos de dados recebidos sao coerentes com o esperado");

					statement.executeUpdate();
					System.out.println("Aviso Escrito");
					return;
				}

				Timestamp Hora;
				int SalaEntrada = rootNode.get("SalaEntrada").asInt();
				int SalaSaida = rootNode.get("SalaSaida").asInt();

				try {
					Hora = Timestamp.valueOf(rootNode.get("Hora").asText());
				} catch (NullPointerException e) {
					Hora = Timestamp.valueOf(rootNode.get("hour").asText());
				} catch (IllegalArgumentException e) {
					// verificar se a data esta em formato certo
					sql = "CALL `insertAlert`(?,?,?,?,?,?)";
					statement = conn.prepareStatement(sql);
					statement.setTimestamp(1, null);
					statement.setInt(2, SalaEntrada);
					statement.setInt(3, 0);
					statement.setInt(4, 0);

					try {
						statement.setString(5, "Data Invalida: " + rootNode.get("Hora").asText());
					} catch (NullPointerException e2) {
						statement.setString(5, "Data Invalida: " + rootNode.get("hour").asText());
					}
					statement.setString(6, "Formato de data nao e o esperado");

					statement.executeUpdate();
					System.out.println("Aviso Escrito");
					return;
				}

				String dateString = Hora.toString();
				Date date;

				try {
					System.out.println(dateString);
					date = dateFormat.parse(dateString);
				} catch (NumberFormatException e) {

					sql = "CALL `insertAlert`(?,?,?,?,?,?)";
					statement = conn.prepareStatement(sql);
					statement.setTimestamp(1, null);
					statement.setInt(2, SalaEntrada);
					statement.setInt(3, 0);
					statement.setInt(4, 0);
					statement.setString(5, "Data Invalida:" + dateString);
					statement.setString(6, "Data em formato errado");
					statement.executeUpdate();
					System.out.println("Aviso Escrito");
					System.out.println("NO CASO DE ESTAR MAL FOI ISTO " + dateString);
					return;

				}

				if (currenttime.after(date)) { // verificar se a data nao e antes da experiencia

					sql = "CALL `insertAlert`(?,?,?,?,?,?)";
					statement = conn.prepareStatement(sql);
					statement.setTimestamp(1, Hora);
					statement.setInt(2, SalaEntrada);
					statement.setInt(3, 0);
					statement.setInt(4, 0);
					statement.setString(5, "Data Invalida");
					statement.setString(6, "Data anterior ao comeco da experiencia");

					statement.executeUpdate();

					System.out.println("Aviso Escrito");

					return;

				}

				// outliers aqui
				statement = conn.prepareStatement(sql);
				statement.setTimestamp(1, Hora);
				statement.setInt(2, SalaEntrada);
				statement.setInt(3, SalaSaida);
				// "Hora": "2023-05-03 18:08:29.817465", "SalaEntrada": 3, "SalaSaida": 2}
				// formato
				statement.executeUpdate();
			}

			System.out.println("Data inserted successfully.");

		} catch (CommunicationsException e) {
			if (topic == true) {
				AddDocumentBackupTemp(doc);
				System.out.println("Inserted in Backup :" + doc.toJson());
			} else {
				AddDocumentBackupMov(doc);
				System.out.println("Inserted in Backup :" + doc.toJson());
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}