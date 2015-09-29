package dataunity.filemetadata.worker;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import asia.stampy.common.message.AbstractBodyMessage;
import asia.stampy.server.message.message.MessageHeader;
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
		private String datatable_iri = null;
		private String path = null;
		private String encoding = null;
		private String dataUnity_base_url = null;
		
		public String getDatatableIRI() {
			return datatable_iri;
		}
		public void setDatatableIRI(String datatableIRI) {
			this.datatable_iri = datatableIRI;
		}

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
	}
	
	public static class ResponseMessage {
		private String datatable_iri = null;
		private String data = null;
		private boolean has_errors = false;
		private String correlation_id = null;
		
		public ResponseMessage(String datatableIRI, String correlationId, String data) {
			setDatatableIRI(datatableIRI);
			setCorrelationId(correlationId);
			setData(data);
		}
		
		public String getDatatableIRI() {
			return datatable_iri;
		}
		public void setDatatableIRI(String datatableIRI) {
			this.datatable_iri = datatableIRI;
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
	
	private static String createResponseMessage(String datatableIRI, String replyTopic, String correlationId, String rdfData) {
		String messageId = java.util.UUID.randomUUID().toString();
		
		// ToDo: Need to figure out how subscription will work with Kafka
		String subscription = "0";
		
		MessageMessage stompMsg = new MessageMessage();
		MessageHeader header = stompMsg.getHeader();
		header.setContentType("text/turtle");
		header.setDestination(replyTopic);
		header.setMessageId(messageId);
		header.setSubscription(subscription);
		header.addHeader("correlation-id", correlationId);
		header.addHeader("datatable-iri", datatableIRI);
		stompMsg.setBody(rdfData);
		String stompMsgStr = stompMsg.toStompMessage(true);
		return stompMsgStr;
	}
	
	private static String createErrorResponseMessage(String datatableIRI, String correlationId, String errorMsg, Exception ex) {
		ResponseMessage msg = new ResponseMessage(datatableIRI, correlationId, errorMsg + ex.getMessage());
		msg.setHasErrors(true);
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
					String errorMsg = createErrorResponseMessage("", "", basicMsg, e);
					// ToDo: send STOMP error message via reply channel 
//					socket.send(errorMsg);
					e.printStackTrace();
					continue;
				}
				
				AbstractBodyMessage stompMsg;
				RequestMessage jobData;
				String correlationId;
				String datatableIRI;
				try {
					// Parse data
					stompMsg = stompMsgParser.parseMessage(taskData);
					// Use message id as Correlation Identifier
					correlationId = stompMsg.getHeader().getHeaderValue("message-id");
					datatableIRI = stompMsg.getHeader().getHeaderValue("datatable-iri");
					jobData = parseRequestMessage(stompMsg.getBody().toString());
				}
				catch (Exception e) {
					// Badly formed message
					// ToDo: replace below with message to error channel?
					String basicMsg = "Problem parsing job data from message.";
					logger.error(basicMsg, e);
					String errorMsg = createErrorResponseMessage("", "", basicMsg, e);
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
				    ResponseMessage response = new ResponseMessage(datatableIRI, correlationId, rdfStr);
				    String responseMsg = createResponseMessage(datatableIRI, replyTopic, correlationId, rdfStr);
				    
				    // Send message to reply channel
				    // ToDo: cache producers?
					simpleProducer.publishMessage(replyTopic, responseMsg);
				}
				catch (Exception e) {
					String basicMsg = "Problem running DataStructDef metadata job.";
					logger.error(basicMsg, e);
					String errorMsg = createErrorResponseMessage(datatableIRI, correlationId, basicMsg, e);
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
}

