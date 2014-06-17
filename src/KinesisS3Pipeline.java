import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.impl.AllPassFilter;
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer;
import com.amazonaws.services.kinesis.connectors.impl.JsonToByteArrayTransformer;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;
import com.amazonaws.services.kinesis.connectors.s3.S3Emitter;
import com.expedia.www.user.interaction.v1.UserInteraction;


public class KinesisS3Pipeline implements IKinesisConnectorPipeline<UserInteraction, byte[]> {

    @Override
    public IEmitter<byte[]> getEmitter(KinesisConnectorConfiguration configuration) {
        return new S3Emitter(configuration);
    }

    @Override
    public IBuffer<UserInteraction> getBuffer(KinesisConnectorConfiguration configuration) {
        return new BasicMemoryBuffer<UserInteraction>(configuration);
    }

    @Override
    public ITransformer<UserInteraction, byte[]> getTransformer(KinesisConnectorConfiguration configuration) {
        return new JsonToByteArrayTransformer<UserInteraction>(UserInteraction.class);
    }

    @Override
    public IFilter<UserInteraction> getFilter(KinesisConnectorConfiguration configuration) {
        return new AllPassFilter<UserInteraction>();
    }
	
}
