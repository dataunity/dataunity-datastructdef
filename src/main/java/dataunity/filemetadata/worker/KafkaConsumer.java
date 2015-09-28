package dataunity.filemetadata.worker;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import asia.stampy.common.message.AbstractBodyMessage;
import asia.stampy.server.message.message.MessageMessage;

import com.google.gson.Gson;
import com.hp.hpl.jena.rdf.model.Model;

import dataunity.datastructdef.MetadataInfo;
import dataunity.stomp.JSONStompMessageParser;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
public class KafkaConsumer {
	private static final Logger logger = Logger.getLogger(KafkaConsumer.class);
	private final ConsumerConnector consumer;
	private final String topic;
	// ToDo: read reply topic from request message?
	private final String replyTopic;
	public KafkaConsumer(String zookeeper, String groupId, String topic, String replyTopic) {
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, groupId));
		this.topic = topic;
		this.replyTopic = replyTopic;
	}
	
	private static class RequestMessage {
		private String path = null;
		private String encoding = null;
		private String dataUnity_base_url = null;
//		private String correlation_id = null;

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
		
//		public String getCorrelationId() {
//			return correlation_id;
//		}
//		public void setCorrelationId(String correlationId) {
//			this.correlation_id = correlationId;
//		}
	}
	
	public static class ResponseMessage {
		private String data = null;
		private boolean has_errors = false;
		private String correlation_id = null;
		
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
	
	private static String createResponseMessage(String replyTopic, String correlationId, String rdfData) {
		String messageId = java.util.UUID.randomUUID().toString();
		
		// ToDo: Need to figure out how subscription will work with Kafka
		String subscription = "0";
		
		MessageMessage stompMsg = new MessageMessage();
		stompMsg.getHeader().setContentType("text/turtle");
		stompMsg.getHeader().setDestination(replyTopic);
		stompMsg.getHeader().setMessageId(messageId);
		stompMsg.getHeader().setSubscription(subscription);
		stompMsg.getHeader().addHeader("correlation-id", correlationId);
		stompMsg.setBody(rdfData);
		String stompMsgStr = stompMsg.toStompMessage(true);
		return stompMsgStr;
	}
	
//	private static String createResponseMessage(ResponseMessage msg) {
//		Gson gson = new Gson();
//		String strMsg = gson.toJson(msg, ResponseMessage.class);
//		return strMsg;
//	}
	
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
		JSONStompMessageParser stompMsgParser = new JSONStompMessageParser();
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
				
				AbstractBodyMessage stompMsg;
				RequestMessage jobData;
				String correlationId;
				try {
					// Parse data
					stompMsg = stompMsgParser.parseMessage(taskData);
					correlationId = stompMsg.getHeader().getHeaderValue("correlation-id");
					jobData = parseRequestMessage(stompMsg.getBody().toString());
				}
				catch (Exception e) {
					// Badly formed message
					// ToDo: replace below with message to error channel?
					String basicMsg = "Problem parsing job data from message.";
					logger.error(basicMsg, e);
					String errorMsg = createErrorResponseMessage("", basicMsg, e);
					// ToDo: send STOMP error message via reply channel 
//					socket.send(errorMsg);
					e.printStackTrace();
					continue;
				}
				
				// Run job
				try {
					String path = jobData.getPath();
					if (path == null) {
						throw new NullPointerException("Request message missing file path");
					}
					String encoding = jobData.getEncoding();
					String dataUnityBaseURL = jobData.getDataUnityBaseURL();
					if (dataUnityBaseURL == null) {
						throw new NullPointerException("Request message missing dataUnityBaseURL");
					}
					
					Model model = MetadataInfo.extractDataStructDef(dataUnityBaseURL, path, encoding);
					ByteArrayOutputStream os = new ByteArrayOutputStream();
				    model.write(os, "TURTLE");
				    String rdfStr = new String(os.toByteArray(), "UTF-8");
				    ResponseMessage response = new ResponseMessage(correlationId, rdfStr);
				    String responseMsg = createResponseMessage(replyTopic, correlationId, rdfStr);
				    
				    // Send message to reply channel
				    // ToDo: cache producers?
					simpleProducer.publishMessage(replyTopic, responseMsg);
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
	public static void main(String[] args) throws IOException {
		// Get config settings
		Properties configProps = new Properties();
		InputStream in = KafkaConsumer.class.getResourceAsStream("/config.properties");
		configProps.load(in);
		in.close();
		
		String zookeeper = null;
		String groupId = configProps.getProperty("consumerGroup");
		String topic = configProps.getProperty("topic");
		String replyTopic = configProps.getProperty("replyTopic");
		
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
		
		KafkaConsumer simpleHLConsumer = new KafkaConsumer(zookeeper, groupId, topic, replyTopic);
		simpleHLConsumer.runConsumer();
	}
}

