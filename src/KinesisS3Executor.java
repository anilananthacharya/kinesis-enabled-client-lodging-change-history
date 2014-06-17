import java.io.IOException;
import java.util.List;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.s3.S3Emitter;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.expedia.www.kinesis.KinesisClient;


public class KinesisS3Executor {
	
	private S3Emitter s3EmitterObj;
	private KinesisClient kinesisClient;
	
	public KinesisS3Executor(KinesisConnectorConfiguration config, KinesisClient kinesisClient) {

		this.kinesisClient = kinesisClient;
		s3EmitterObj = new S3Emitter(config);
		
	}
	
	public void kinesisS3Emit(String streamName) {

		AmazonKinesisClient amazonKinesisClient = kinesisClient.getAmazonKinesisClient();
		
		List<Shard> shards = null;
		try {
			shards = kinesisClient.getShards(streamName);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	String primaryShardId = shards.get(0).getShardId();
    	
    	String shardIterator;
    	GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
    	getShardIteratorRequest.setStreamName(streamName);
    	getShardIteratorRequest.setShardId(primaryShardId);
    	getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");
    	GetShardIteratorResult getShardIteratorResult = amazonKinesisClient.getShardIterator(getShardIteratorRequest);
    	shardIterator = getShardIteratorResult.getShardIterator();
    	
    	while(true) {

    		//Create new GetRecordsRequest with existing shardIterator.
    		//Set maximum records to return to 1000.
    		GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
    		getRecordsRequest.setShardIterator(shardIterator);
    		getRecordsRequest.setLimit(1000); 

    		GetRecordsResult result = amazonKinesisClient.getRecords(getRecordsRequest);
    		
    		List <Record> records = result.getRecords();

			if(records == null) {
				System.out.println("Get returned NULL");
			} else {
				for(int i = 0; i < records.size(); i ++) {
					Record singleRecord = records.get(i);
					String seqNum = singleRecord.getSequenceNumber();
					System.out.println(seqNum);
					byte[] recordInBytes = singleRecord.getData().array();
					try {
						s3EmitterObj.unbufferedEmit(recordInBytes, seqNum);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			
    		try {
    			Thread.sleep(1000);
    		} 
    		catch (InterruptedException exception) {
    			throw new RuntimeException(exception);
    		}
    		
    		shardIterator = result.getNextShardIterator();
    		
    		if(shardIterator == null) {
    			break;
    		}
    	}

	}

}
