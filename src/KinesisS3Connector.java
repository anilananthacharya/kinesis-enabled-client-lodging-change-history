import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.expedia.www.kinesis.KinesisClient;
import com.expedia.www.kinesis.KinesisClientConfig;
import com.expedia.www.kinesis.KinesisStateManager;

public class KinesisS3Connector implements Runnable {

	private String kinesisEndpointUrl;
	private String streamName;
	private KinesisClient kinesisClient; 
	
	public KinesisS3Connector (String kinesisEndpointUrl, String streamName) {
		this.kinesisEndpointUrl = kinesisEndpointUrl;
		this.streamName = streamName;

		List<String> kinesisStreamList = new ArrayList<String>();
		kinesisStreamList.add(streamName);
		
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

		String s3BucketName = "lodging-change-history";
		
		Properties prop = new Properties();
		prop.put(KinesisConnectorConfiguration.PROP_KINESIS_ENDPOINT, kinesisEndpointUrl);
		prop.put(KinesisConnectorConfiguration.PROP_KINESIS_INPUT_STREAM, streamName);
		prop.put(KinesisConnectorConfiguration.PROP_S3_BUCKET, s3BucketName);
		
		KinesisConnectorConfiguration config = new KinesisConnectorConfiguration(prop, new DefaultAWSCredentialsProviderChain());

		KinesisS3Executor kinesisS3ExecutorObject = new KinesisS3Executor(config, kinesisClient);
		kinesisS3ExecutorObject.kinesisS3Emit(streamName);
	}
	
}
