package mongoToSql;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.bson.Document;

public class MongoDBReader {

	private final MongoCollection<Document> BackupCollection;
	private final MongoCollection<Document> collection;
	private final Timer timer;
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public MongoDBReader(String dbName, String collectionName) {
		this.collection = MongoClients.create("mongodb://localhost:27017").getDatabase(dbName)
				.getCollection(collectionName);
		this.BackupCollection = MongoClients.create("mongodb://localhost:27017").getDatabase("Backup")
				.getCollection(collectionName);
		this.timer = new Timer();

	}

	public void RemoveBackupMongoDB() {
		BackupCollection.deleteMany(new Document());
	}

	public void RemoveMongoDB() {
		collection.deleteMany(new Document());
	}

	public void startReadingAndWriting(final Boolean IsTemp) {
		TimerTask task = new TimerTask() {
			@Override
			public void run() {
				MongoCursor<Document> cursor = collection.find().iterator();
				MongoCursor<Document> BackupCursor = BackupCollection.find().iterator();
				if (BackupCollection.countDocuments() == 0) {
					while (cursor.hasNext()) {
						Document doc = cursor.next();
						LocalDateTime currentDateTime = LocalDateTime.now();
						Date currentDate = Date.from(currentDateTime.atZone(ZoneId.systemDefault()).toInstant());
						String currentTimeFormatted = dateFormat.format(currentDate);
						try {
							Date niceTime = dateFormat.parse(currentTimeFormatted);
							JsonToMysql.insertDataFromJson(doc.toJson(), IsTemp, niceTime, doc);
							RemoveMongoDB();
						} catch (ParseException e) {
							e.printStackTrace();
						}
						System.out.println(doc.toJson());
					}
				}
				if (BackupCollection.countDocuments() > 0) {
					while (BackupCursor.hasNext()) {
						Document doc = BackupCursor.next();
						LocalDateTime currentDateTime = LocalDateTime.now();
						Date currentDate = Date.from(currentDateTime.atZone(ZoneId.systemDefault()).toInstant());
						String currentTimeFormatted = dateFormat.format(currentDate);
						try {
							Date niceTime = dateFormat.parse(currentTimeFormatted);
							JsonToMysql.insertDataFromBackup(doc.toJson(), IsTemp, niceTime, doc);
						} catch (ParseException e) {
							e.printStackTrace();
						}
						System.out.println(doc.toJson());
					}
				}
			}
		};
		timer.schedule(task, 0, 1000);
	}

	public static void main(String[] args) {
		MongoDBReader reader = new MongoDBReader("pisid", "movimentacaoRatos");
		reader.startReadingAndWriting(false);
		MongoDBReader readerTemp = new MongoDBReader("pisid", "temperatura");
		readerTemp.startReadingAndWriting(true);
	}
}
