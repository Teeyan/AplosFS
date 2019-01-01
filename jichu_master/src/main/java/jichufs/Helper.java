package jichufs;

import lombok.var;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class Helper {

    /**
     * Adds recent updates to Message.
     * @param builder
     * @return
     */
    @Autowired
    private  MembershipList membershipList;

    public  FSMessages.Message.Builder addEventUpdates(FSMessages.Message.Builder builder){
        var recentUpdates = membershipList.getRecentUpdates();
        builder.setData(false);
        if(recentUpdates.size() > 0){
            builder.setData(true);
            builder.addAllNodeEvents(recentUpdates);
        }
        return builder;
    }
}
