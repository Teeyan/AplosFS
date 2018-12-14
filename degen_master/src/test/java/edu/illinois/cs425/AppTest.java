package edu.illinois.cs425;


import com.jcabi.ssh.Shell;
import com.jcabi.ssh.Ssh;
import org.junit.*;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class AppTest {

    private int port;
    private byte[] bytes;
    private List<String> machineIds;

    private void setupServer() throws Exception {

        final File f = new File("C:/Users/aniru/.ssh/id_rsa");

        this.bytes = Files.readAllBytes(f.toPath());

        final Properties config = new Properties();
        config.load(new FileInputStream("mp2AppConfig"));

        machineIds = Arrays.asList(config.getProperty("machineIds").split(","));
//
//        for(String machineId: machineIds){
//            final Shell shell = new Ssh(machineId, 22, "anain2", new String(this.bytes, StandardCharsets.UTF_8.name()));
//            new Shell.Plain(shell).exec("kill -9 $(/usr/sbin/lsof -t -i:8091)");
//            new Shell.Plain(shell).exec("java -cp cs425.jar edu.illinois.cs425.App");
//        }
    }

//    @Before
//    public void initialize() throws Exception {
//        this.port = 8091;
//        setupServer();
//    }
//
//    @After
//    public void cleanUpServer() throws Exception {
//        for(String machineId: this.machineIds) {
//            final Shell shell = new Ssh(machineId, 22, "anain2", new String(this.bytes, StandardCharsets.UTF_8.name()));
//            new Shell.Plain(shell).exec("kill -9 $(/usr/sbin/lsof -t -i:8091 -sTCP:LISTEN)");
//        }
//    }

    @Test
    public void testSuccessCase() throws Exception{
        System.out.print("Copied File");
    }
}