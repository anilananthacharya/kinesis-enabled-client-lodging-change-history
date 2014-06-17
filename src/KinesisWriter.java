import java.util.ArrayList;
import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.expedia.www.kinesis.KinesisClient;
import com.expedia.www.kinesis.KinesisClientConfig;
import com.expedia.www.kinesis.KinesisStateManager;
import com.expedia.www.kinesis.OperationResult;


public class KinesisWriter implements Runnable {

	private byte[] messageInBytes;
	private KinesisClient kinesisClient;
	
	public KinesisWriter(String kinesisEndpointUrl, String kinesisStreamName, byte[] messageInBytes) {
		
		this.messageInBytes = messageInBytes;
		
		List<String> kinesisStreamList = new ArrayList<String>();
		kinesisStreamList.add(kinesisStreamName);
		
		ClientConfiguration clientConfiguration = null;
		KinesisClientConfig kinesisClientConfig = new KinesisClientConfig(kinesisStreamList, clientConfiguration, kinesisEndpointUrl);

		KinesisStateManager kinesisStateManager = null;
		try {
			// client state
			kinesisStateManager = new KinesisStateManager(kinesisClientConfig);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// Initialize kinesis enabled client
		this.kinesisClient = new KinesisClient(kinesisClientConfig, kinesisStateManager);

	}
	
	
	
	@Override
	public void run() {
		
		try {
			List<OperationResult<PutRecordResult>> resultSet = kinesisClient.putRecord(messageInBytes);
			for (OperationResult<PutRecordResult> result : resultSet) {
				if (result.isSuccessful()) {
					// log result.getStreamId(); result.getResult();
					System.out.println("Put successful - stream Id = " + result.getStreamId());
					System.out.println("Result = " + result.getResult());
				} else {
					// log result.getStreamId(); result.getError();
					System.out.println("Put failed - stream Id = " + result.getStreamId());
					System.out.println("Error = " + result.getError());
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
}
