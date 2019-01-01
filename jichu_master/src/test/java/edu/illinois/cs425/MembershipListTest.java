package edu.illinois.cs425;

import junit.framework.Test;
import junit.framework.TestCase;

import java.util.List;

public class MembershipListTest extends TestCase {

    public void testGetNextN(){
        MembershipList ml = new MembershipList();
        ml.setHostId("5");
        ml.add("1");
        ml.add("2");
        ml.add("3");
        ml.add("5");
        ml.add("6");
        ml.getRandomNeighbour(ml.getNextNEntries(3)).getId();
        ml.add("4");
        List<Member> next = ml.getNextNEntries(3);
        for(Member m: next){
            System.out.println(m.getId());
        }
        ml.remove("6");
        next = ml.getNextNEntries(3);
        for(Member m: next){
            System.out.println(m.getId());
        }
    }
}
