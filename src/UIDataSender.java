import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class UIDataSender {

	public static void main(String[] args) {
		
		// Create a User interaction object and add sample data to it.
		
		MessageModel messageModel = new MessageModel();
		
		byte[] messageInBytes = messageModel.getMessageInBytes();

		// Add UI data to kinesis stream
		
		String kinesisEndpointUrl = "https://kinesis.us-east-1.amazonaws.com";
		String streamName = "lodging-inventory-test";
		
		int numOfThreads = 5;
		ExecutorService executor = Executors.newFixedThreadPool(numOfThreads);
		
		Runnable kinesisWriterRunnable = new KinesisWriter(kinesisEndpointUrl, streamName, messageInBytes);
		executor.execute(kinesisWriterRunnable);
		
		Runnable KinesisS3ConnectorRunnable = new KinesisS3Connector(kinesisEndpointUrl, streamName);
		executor.execute(KinesisS3ConnectorRunnable);
		
		executor.shutdown();
	}

}
