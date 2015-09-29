package dataunity.filemetadata.worker;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

public class Main {
	private static final Logger logger = Logger.getLogger(Main.class);

	public static void main(String[] args) throws IOException {
		// Get config settings
		Properties configProps = new Properties();
		InputStream in = KafkaConsumer.class.getResourceAsStream("/config.properties");
		configProps.load(in);
		in.close();
		
		String groupId = configProps.getProperty("consumerGroup");
		String topic = configProps.getProperty("topic");
		String replyTopic = configProps.getProperty("replyTopic");
		
		// Read docker environment variables
		Map<String, String> env = System.getenv();
		String zookeeper_addr = env.get("ZOOKEEPER_PORT_2181_TCP_ADDR");
		String zookeeper_port = env.get("ZOOKEEPER_PORT_2181_TCP_PORT");
//		logger.info(String.format("Using ZooKeeper addr %s", zookeeper_addr));
//		logger.info(String.format("Using ZooKeeper port %s", zookeeper_port));
//		System.out.flush();
		String zookeeper = null;
		if (zookeeper_addr != null && !zookeeper_addr.isEmpty() &&
				zookeeper_port != null && !zookeeper_port.isEmpty()) {
			zookeeper = String.format("%s:%s", zookeeper_addr, zookeeper_port);
			logger.info(String.format("Using ZooKeeper endpoint %s", zookeeper));
		}
		if (zookeeper == null) {
			throw new NullPointerException("Zookeeper endpoint should be specified through environment.");
		}
		
		KafkaConsumer simpleHLConsumer = new KafkaConsumer(zookeeper, groupId, topic, replyTopic);
		simpleHLConsumer.runConsumer();
	}

}