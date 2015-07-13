//package dataunity.filemetadata.worker;
//
//import java.util.Map;
//
//import org.apache.log4j.Logger;
//
//import dataunity.filemetadata.worker.Worker;
//
// OLD ZEROMQ VERSION
//public class Main {
//	private static final Logger logger = Logger.getLogger(Worker.class);
//
//	public static void main(String[] args) {
//		// TODO Auto-generated method stub
//		String endPoint = "tcp://127.0.0.1:5004";
//		
//		// Override docker environment variables
//		Map<String, String> env = System.getenv();
//		String filemetadata_addr = env.get("FILEMETADATA_PORT_5004_TCP_ADDR");
//		String filemetadata_port = env.get("FILEMETADATA_PORT_5004_TCP_PORT");
//		if (filemetadata_addr != null && !filemetadata_addr.isEmpty() &&
//				filemetadata_port != null && !filemetadata_port.isEmpty()) {
//			endPoint = String.format("tcp://%s:%s", filemetadata_addr, filemetadata_port);
//			System.out.println(String.format("Set endpoint to %s", endPoint));
//		}
//		logger.info(String.format("Starting filemetadata worker on endpoint %s", endPoint));
//		Worker worker = new Worker();
//		worker.start(endPoint);
//	}
//
//}
