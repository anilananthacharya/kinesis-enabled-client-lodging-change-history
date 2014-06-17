import java.util.Arrays;

import com.expedia.www.user.interaction.v1.DomainEntity;
import com.expedia.www.user.interaction.v1.LodgingChangeHistory;
import com.expedia.www.user.interaction.v1.ProductTypeEnum;
import com.expedia.www.user.interaction.v1.Site;
import com.expedia.www.user.interaction.v1.User;
import com.expedia.www.user.interaction.v1.UserAndSessionContext;
import com.expedia.www.user.interaction.v1.UserInteraction;


public class MessageModel {
	
	private UserInteraction userInteraction;
	
	public MessageModel() {

		userInteraction = new UserInteraction();
		
		userInteraction.context = new UserAndSessionContext();
		userInteraction.context.user = new User();
		userInteraction.context.user.guid = "100";
		userInteraction.context.site = new Site();
		userInteraction.context.site.siteId = 1;

		userInteraction.setUtcTimestamp(System.currentTimeMillis());
		
		userInteraction.entity = new DomainEntity();
		userInteraction.entity.productTypes = Arrays.asList(ProductTypeEnum.LODGING);
		userInteraction.entity.lodgingChangeHistory = new LodgingChangeHistory();
		userInteraction.entity.lodgingChangeHistory.changeRequestId = 123;
		userInteraction.entity.lodgingChangeHistory.bookingId = 345;
		userInteraction.entity.lodgingChangeHistory.stayDate = "01/15/2014";
		userInteraction.entity.lodgingChangeHistory.startDate = "01/01/2014";
		userInteraction.entity.lodgingChangeHistory.endDate = "01/31/2014";
	
	}

	byte[] getMessageInBytes() {
		
		return userInteraction.toString().getBytes();
	
	}
	
}
