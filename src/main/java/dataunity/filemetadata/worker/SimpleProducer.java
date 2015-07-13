package dataunity.filemetadata.worker;

import java.util.Date;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SimpleProducer {
	private static final Logger logger = Logger.getLogger(SimpleProducer.class);
	private static Producer<String, String> producer;
	public SimpleProducer() {
		String kafka_host = null;
		Properties props = new Properties();
		Map<String, String> env = System.getenv();
		String kafka_addr = env.get("KAFKA_PORT_9092_TCP_ADDR");
		String kafka_port = env.get("KAFKA_PORT_9092_TCP_PORT");
		logger.info(String.format("Using Kafka addr %s", kafka_addr));
		logger.info(String.format("Using Kafka port %s", kafka_port));
		System.out.flush();
		if (kafka_addr != null && !kafka_addr.isEmpty() &&
				kafka_port != null && !kafka_port.isEmpty()) {
			kafka_host = String.format("%s:%s", kafka_addr, kafka_port);
			logger.info(String.format("Using Kafka endpoint %s", kafka_host));
		}
		if (kafka_host == null) {
			throw new NullPointerException("Kafka endpoint should be specified through environment.");
		}
		
		// Set the broker list for requesting metadata to find the lead broker
//		props.put("metadata.broker.list", "192.168.146.132:9092, 192.168.146.132:9093, 192.168.146.132:9094");
//		props.put("metadata.broker.list", "172.17.0.2:9092");
//		props.put("metadata.broker.list", "localhost:9092");
		props.put("metadata.broker.list", kafka_host);
//		props.put("metadata.broker.list", "localhost:7203");
		//This specifies the serializer class for keys
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// 1 means the producer receives an acknowledgment once the lead replica
		// has received the data. This option provides better durability as the
		// client waits until the server acknowledges the request as successful.
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
	}
//	public static void main(String[] args) {
//		int argsCount = args.length;
//		if (argsCount == 0 || argsCount == 1)
//		throw new IllegalArgumentException(
//		"Please provide topic name and Message count as arguments");
//		// Topic name and the message count to be published is passed from the
//		// command line
//		String topic = (String) args[0];
//		String count = (String) args[1];
//		int messageCount = Integer.parseInt(count);
//		System.out.println("Topic Name - " + topic);
//		System.out.println("Message Count - " + messageCount);
//		SimpleProducer simpleProducer = new SimpleProducer();
//		simpleProducer.publishMessage(topic, messageCount);
//	}
	public void publishMessage(String topic, String message) {
		//for (int mCount = 0; mCount < messageCount; mCount++) {
//		String runtime = new Date().toString();
//		String msg = "Message Publishing Time - " + runtime;
//		System.out.println(msg);
		// Creates a KeyedMessage instance
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, message);
		// Publish the message
		producer.send(data);
		logger.info(String.format("Sent message to topic %s: %s", topic, message));
		//}
		
	}
	
	public void close() {
		// Close producer connection with broker.
		producer.close();
	}
}
