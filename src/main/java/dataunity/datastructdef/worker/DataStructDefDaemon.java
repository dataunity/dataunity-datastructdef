//package dataunity.datastructdef.worker;
//
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.util.Properties;
//
//import org.apache.commons.daemon.Daemon;
//import org.apache.commons.daemon.DaemonContext;
//import org.apache.commons.daemon.DaemonInitException;
//import org.apache.log4j.Logger;
//
//public class DataStructDefDaemon implements Daemon {
//	private static final Logger log = Logger.getLogger(DataStructDefDaemon.class);
//	private DataStructDefWorker worker = new DataStructDefWorker();
//	String endPoint = "tcp://127.0.0.1:5004";
//
//	public void destroy() {
//		// TODO Auto-generated method stub
//		
//	}
//
//	public void init(DaemonContext arg0) throws DaemonInitException, Exception {
//		// TODO Auto-generated method stub
//		String[] args = arg0.getArguments();
//		
//	    if(args.length > 1)
//	    {
//	    	log.error("Max of one argument is accepted: config-filepath");
//	        System.exit(0);
//	    }
//	    else if (args.length == 1) {
//	    	// Load config file
//	    	log.info("Using config file for file metadata daemon");
//	    	String filepath = args[0];
//	    	Properties properties = new Properties();
//	    	try {
//				properties.load(new FileInputStream(filepath));
//				String configEndPoint = properties.getProperty("du.filemetadata.endPoint");
//				if(configEndPoint != null && !configEndPoint.isEmpty()) {
//					endPoint = configEndPoint;
//				}
//	    	} catch (IOException e) {
//	    		log.error("Could not read the config file specified in args", e);
//		        System.exit(0);
//	    	}
//		}
//	}
//
//	public void start() throws Exception {
//		// TODO Auto-generated method stub
//		log.info(String.format("Starting file metadata daemon with endpoint %s", endPoint));
//		worker.start(endPoint);
//		
//	}
//
//	public void stop() throws Exception {
//		// TODO Auto-generated method stub
//		log.info(String.format("Stopping file metadata daemon"));
//		worker.shutdown();
//	}
//
//}
