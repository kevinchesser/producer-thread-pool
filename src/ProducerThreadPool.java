import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.File;

public class ProducerThreadPool {
	public static void main(String[] args) {
		File directory = new File("path to directory");
		File[] fList = directory.listFiles();
		ExecutorService executor = Executors.newFixedThreadPool(fList.length);
		for (File file : fList) {
			Runnable worker = new ProducerThread("hostname:port", "kafkatopic", file.getName(),
					directory);
			executor.execute(worker);
		}
		executor.shutdown();
		while (!executor.isTerminated()) {
		}
		System.out.println("Finished all threads");
	}
}