package dataunity.datastructdef.worker;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;

public class DataStructDefDaemon implements Daemon {
	private DataStructDefWorker worker = new DataStructDefWorker();

	public void destroy() {
		// TODO Auto-generated method stub
		
	}

	public void init(DaemonContext arg0) throws DaemonInitException, Exception {
		// TODO Auto-generated method stub
		
	}

	public void start() throws Exception {
		// TODO Auto-generated method stub
		String endPoint = "tcp://127.0.0.1:5004";
		worker.start(endPoint);
		
	}

	public void stop() throws Exception {
		// TODO Auto-generated method stub
		worker.shutdown();
	}

}
