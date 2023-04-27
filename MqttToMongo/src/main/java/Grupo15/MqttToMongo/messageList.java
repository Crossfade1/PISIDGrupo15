package Grupo15.MqttToMongo;

import java.util.ArrayList;
import java.util.List;

public class messageList {

	public List<mensagemMQTT> backupList = new ArrayList<>();
	
	public messageList() {
		// TODO Auto-generated constructor stub
		
	}
	
	public mensagemMQTT get() throws InterruptedException {
		while(backupList.isEmpty())
			wait();
		return backupList.remove(0);
	}
	
	public void put(mensagemMQTT msg) {
		backupList.add(msg);
		notifyAll();
	}

}
