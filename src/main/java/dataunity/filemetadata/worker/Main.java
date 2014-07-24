package dataunity.filemetadata.worker;

import dataunity.filemetadata.worker.Worker;

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String endPoint = "tcp://127.0.0.1:5004";
		Worker worker = new Worker();
		worker.start(endPoint);
	}

}
