package dataunity.filemetadata.worker;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.hp.hpl.jena.rdf.model.Model;

import dataunity.datastructdef.MetadataInfo;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
public class KafkaConsumer {
	private static final Logger logger = Logger.getLogger(KafkaConsumer.class);
	private final ConsumerConnector consumer;
	private final String topic;
	public KafkaConsumer(String zookeeper, String groupId, String topic) {
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, groupId));
		this.topic = topic;
	}
	
	private static class RequestMessage {
		private String path = null;
		private String encoding = null;
		private String dataUnity_base_url = null;
		private String correlation_id = null;

		public String getPath() {
			return path;
		}
		public void setPath(String path) {
			this.path = path;
		}
		
		public String getEncoding() {
			return encoding;
		}
		public void setEncoding(String encoding) {
			this.encoding = encoding;
		}
		
		public String getDataUnityBaseURL() {
			return dataUnity_base_url;
		}
		public void setDataUnityBaseURL(String dataUnityBaseURL) {
			this.dataUnity_base_url = dataUnityBaseURL;
		}
		
		public String getCorrelationId() {
			return correlation_id;
		}
		public void setCorrelationId(String correlationId) {
			this.correlation_id = correlationId;
		}
	}
	
	public static class ResponseMessage {
		private String data = null;
		private boolean has_errors = false;
		private String correlation_id = null;
		
//		public ResponseMessage() {
//		}
		
		public ResponseMessage(String correlationId, String data) {
			setCorrelationId(correlationId);
			setData(data);
		}

		public String getData() {
			return data;
		}
		public void setData(String data) {
			this.data = data;
		}
		
		public boolean getHasErrors() {
			return has_errors;
		}
		public void setHasErrors(boolean hasErrors) {
			this.has_errors = hasErrors;
		}
		
		public String getCorrelationId() {
			return correlation_id;
		}
		public void setCorrelationId(String correlationId) {
			this.correlation_id = correlationId;
		}
	}

	private static RequestMessage parseRequestMessage(String msg) {
		Gson gson = new Gson();
		RequestMessage requestMsg = gson.fromJson(msg, RequestMessage.class);
		return requestMsg;
	}
	
	private static String createResponseMessage(ResponseMessage msg) {
		Gson gson = new Gson();
		String strMsg = gson.toJson(msg, ResponseMessage.class);
		return strMsg;
	}
	
	private static String createErrorResponseMessage(String correlationId, String errorMsg, Exception ex) {
		ResponseMessage msg = new ResponseMessage(correlationId, errorMsg + ex.getMessage());
		msg.setHasErrors(true);
//		msg.setData(errorMsg + ex.getMessage());
//		msg.setCorrelationId(correlationId);
		Gson gson = new Gson();
		String strMsg = gson.toJson(msg, ResponseMessage.class);
		return strMsg;
	}
	
	private static ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", groupId);
//		props.put("zookeeper.session.timeout.ms", "500");
//		props.put("zookeeper.sync.time.ms", "250");
//		props.put("auto.commit.interval.ms", "1000");
		return new ConsumerConfig(props);
	}
	public void runConsumer() {
		SimpleProducer simpleProducer = new SimpleProducer();
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		// Define single thread for topic
		topicMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamsMap = consumer.createMessageStreams(topicMap);
		List<KafkaStream<byte[], byte[]>> streamList = consumerStreamsMap.get(topic);
		for (final KafkaStream<byte[], byte[]> stream : streamList) {
			ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
			while (consumerIte.hasNext()) {
				String taskData;
				try {
					taskData = new String(consumerIte.next().message());
					System.out.println("Got message");
					System.out.flush();
					logger.info("Got message");
					logger.info(taskData);
				}
				catch (Exception e) {
					// Badly formed message
					// ToDo: replace below with message to error channel?
					String basicMsg = "Problem running file metadata job.";
					System.out.println("Error getting message");
					System.out.flush();
					logger.error(basicMsg, e);
					String errorMsg = createErrorResponseMessage("", basicMsg, e);
					// ToDo: send STOMP error message via reply channel 
//					socket.send(errorMsg);
					e.printStackTrace();
					continue;
				}
				
				// Run job
				String correlationId = "";
				try {
					RequestMessage jobData = parseRequestMessage(taskData);
					correlationId = jobData.getCorrelationId();
					String path = jobData.getPath();
					if (path == null) {
						throw new RuntimeException("Request message missing file path");
					}
					String encoding = jobData.getEncoding();
					String dataUnityBaseURL = jobData.getDataUnityBaseURL();
					if (dataUnityBaseURL == null) {
						throw new RuntimeException("Request message missing dataUnityBaseURL");
					}
					
					Model model = MetadataInfo.extractDataStructDef(dataUnityBaseURL, path, encoding);
					ByteArrayOutputStream os = new ByteArrayOutputStream();
				    model.write(os, "TURTLE");
				    String rdfStr = new String(os.toByteArray(), "UTF-8");
				    ResponseMessage response = new ResponseMessage(correlationId, rdfStr);
				    String responseMsg = createResponseMessage(response);
				    
				    // Send message to reply channel
				    // ToDo: cache producers?
					String reply_topic = "test1reply";
					
					simpleProducer.publishMessage(reply_topic, responseMsg);
				}
				catch (Exception e) {
					String basicMsg = "Problem running DataStructDef metadata job.";
					logger.error(basicMsg, e);
					String errorMsg = createErrorResponseMessage(correlationId, basicMsg, e);
					// ToDo: send STOMP error message via reply channel 
//					socket.send(errorMsg);
					e.printStackTrace();
					continue;
				}
//				System.out.println("Message from Single Topic :: " + new String(consumerIte.next().message()));
			}
		}
		logger.info("Closing Kafka consumer");
		System.out.flush();
		if (consumer != null)
			consumer.shutdown();
		simpleProducer.close();
	}
	public static void main(String[] args) {
//		String zooKeeper = args[0];
//		String groupId = args[1];
//		String topic = args[2];
		String zookeeper = null;
		String groupId = "filemetadatagroup";
		String topic = "test1";
		// Read docker environment variables
		Map<String, String> env = System.getenv();
		String zookeeper_addr = env.get("ZOOKEEPER_PORT_2181_TCP_ADDR");
		String zookeeper_port = env.get("ZOOKEEPER_PORT_2181_TCP_PORT");
//		logger.info(String.format("Using ZooKeeper addr %s", zookeeper_addr));
//		logger.info(String.format("Using ZooKeeper port %s", zookeeper_port));
		System.out.flush();
		if (zookeeper_addr != null && !zookeeper_addr.isEmpty() &&
				zookeeper_port != null && !zookeeper_port.isEmpty()) {
			zookeeper = String.format("%s:%s", zookeeper_addr, zookeeper_port);
			logger.info(String.format("Using ZooKeeper endpoint %s", zookeeper));
		}
		if (zookeeper == null) {
			throw new NullPointerException("Zookeeper endpoint should be specified through environment.");
		}
		
		KafkaConsumer simpleHLConsumer = new KafkaConsumer(zookeeper, groupId, topic);
		simpleHLConsumer.runConsumer();
	}
}

