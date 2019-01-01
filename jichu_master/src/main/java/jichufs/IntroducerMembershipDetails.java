package jichufs;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@Builder
public class IntroducerMembershipDetails implements Serializable {

    private String id;
    private List<Member> membersList;
    private Member master;
}