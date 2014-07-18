package dataunity.datastructdef.worker;

public class DataStructDefMain {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String endPoint = "tcp://127.0.0.1:5004";
		DataStructDefWorker worker = new DataStructDefWorker();
		worker.start(endPoint);
	}

}

