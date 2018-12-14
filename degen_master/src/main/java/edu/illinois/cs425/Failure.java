package degenfs;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import spring.SpringConfig;

import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.time.Instant;
import java.util.stream.Collectors;
import java.io.*;

import java.util.*;
import java.net.*;
import degenfs.FSMessages.*;


public class Failure implements Runnable {
	
	private Member failedMember;
	@Autowired
	private Master curMaster;
	@Autowired
    private Member master;
    @Autowired
    private Election election;


	public Failure(Member failedMember) {
		this.failedMember = failedMember;
	}

	
	@Override
	public void run() {
		// Start the Failure thread to 
        if (failedMember.getId().equals(master.getId())) {
            election.initiate();
        }

        else if (curMaster.getHostId() != null) {
            curMaster.rectifyNodeFailure(failedMember);
        }
	}

}