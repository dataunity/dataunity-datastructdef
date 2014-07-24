//package dataunity.datastructdef.worker;
//
//import java.io.ByteArrayOutputStream;
//
//import org.apache.log4j.Logger;
//import org.zeromq.ZMQ;
//
//import com.google.gson.Gson;
//import com.hp.hpl.jena.rdf.model.Model;
//
//import dataunity.datastructdef.MetadataInfo;
//
//public class DataStructDefWorker {
//	private static final Logger logger = Logger.getLogger(DataStructDefWorker.class);
//	private boolean isStopped = true;
//	
//	public void shutdown() {
//		logger.info("Setting shutdown flag for DataStructDef service.");
//		isStopped = true;
//	}
//	
//	private static class RequestMessage {
//		private String path = null;
//		private String encoding = null;
//		private String dataUnity_base_url = null;
//
//		public String getPath() {
//			return path;
//		}
//		public void setPath(String path) {
//			this.path = path;
//		}
//		
//		public String getEncoding() {
//			return encoding;
//		}
//		public void setEncoding(String encoding) {
//			this.encoding = encoding;
//		}
//		
//		public String getDataUnityBaseURL() {
//			return dataUnity_base_url;
//		}
//		public void setDataUnityBaseURL(String dataUnityBaseURL) {
//			this.dataUnity_base_url = dataUnityBaseURL;
//		}
//	}
//	
//	public static class ResponseMessage {
//		private String data = null;
//		private boolean has_errors = false;
//		
//		public ResponseMessage() {
//		}
//		
//		public ResponseMessage(String data) {
//			setData(data);
//		}
//
//		public String getData() {
//			return data;
//		}
//		public void setData(String data) {
//			this.data = data;
//		}
//		
//		public boolean getHasErrors() {
//			return has_errors;
//		}
//		public void setHasErrors(boolean hasErrors) {
//			this.has_errors = hasErrors;
//		}
//	}
//
//	private static RequestMessage parseRequestMessage(String msg) {
//		Gson gson = new Gson();
//		RequestMessage requestMsg = gson.fromJson(msg, RequestMessage.class);
//		return requestMsg;
//	}
//	
//	private static String createResponseMessage(ResponseMessage msg) {
//		Gson gson = new Gson();
//		String strMsg = gson.toJson(msg, ResponseMessage.class);
//		return strMsg;
//	}
//	
//	private static String createErrorResponseMessage(String errorMsg, Exception ex) {
//		ResponseMessage msg = new ResponseMessage();
//		msg.setHasErrors(true);
//		msg.setData(errorMsg + ex.getMessage());
//		Gson gson = new Gson();
//		String strMsg = gson.toJson(msg, ResponseMessage.class);
//		return strMsg;
//	}
//	
//	public void start(String endPoint) {
//		System.out.println("Starting DataStructDef service.");
//		logger.info("Starting DataStructDef service.");
//		if (!isStopped) {
//			// Error if already running
//			throw new RuntimeException("DataStructDef Service already started.");
//		}
//		isStopped = false;
//		
//		ZMQ.Context context = ZMQ.context(1);
//		ZMQ.Socket socket = context.socket(ZMQ.REP);
//		socket.bind(endPoint);
//		
//		while (!isStopped) {
//			// Get message
//			String taskData;
//			try {
//				taskData = socket.recvStr();
//			}
//			catch (Exception e) {
//				String basicMsg = "Problem running DataStructDef metadata job.";
//				logger.error(basicMsg, e);
//				String errorMsg = createErrorResponseMessage(basicMsg, e);
//				socket.send(errorMsg);
//				e.printStackTrace();
//				continue;
//			}
//			
//			// Run job
//			try {
//				RequestMessage jobData = parseRequestMessage(taskData);
//				String path = jobData.getPath();
//				if (path == null) {
//					throw new RuntimeException("Request message missing file path");
//				}
//				String encoding = jobData.getEncoding();
//				String dataUnityBaseURL = jobData.getDataUnityBaseURL();
//				if (dataUnityBaseURL == null) {
//					throw new RuntimeException("Request message missing dataUnityBaseURL");
//				}
//				
//				Model model = MetadataInfo.extractDataStructDef(dataUnityBaseURL, path, encoding);
//				ByteArrayOutputStream os = new ByteArrayOutputStream();
//			    model.write(os, "TURTLE");
//			    String rdfStr = new String(os.toByteArray(), "UTF-8");
//			    ResponseMessage response = new ResponseMessage(rdfStr);
//			    String responseMsg = createResponseMessage(response);
//			    socket.send(responseMsg);
//			}
//			catch (Exception e) {
//				String basicMsg = "Problem running DataStructDef metadata job.";
//				logger.error(basicMsg, e);
//				String errorMsg = createErrorResponseMessage(basicMsg, e);
//				socket.send(errorMsg);
//				e.printStackTrace();
//				continue;
//			}
//			
//		}
//
//		socket.close();
//		context.term();
//		logger.info("Exiting DataStructDef service.");
//	}
//	
//}
