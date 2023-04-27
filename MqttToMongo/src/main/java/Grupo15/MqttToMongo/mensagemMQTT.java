package Grupo15.MqttToMongo;

public class mensagemMQTT {
	
	private String topic;
	private String mensagem;
	
	public mensagemMQTT(String topic, String mensagem) {
		this.topic=topic;
		this.mensagem=mensagem;
	}
	
	public String getTopic() {
		return this.topic;
	}
	
public String getMensagem() {
		return this.mensagem;
	}

}
