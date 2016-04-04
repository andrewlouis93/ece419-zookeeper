import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.util.concurrent.CountDownLatch;
import java.util.List;
import java.net.*;
import java.io.*;


public class JobTracker {
    
    String jobTrackerPath = "/jobtracker";
    String jobsPath = "/jobs";
    ZkConnector zkc;
    Watcher watcher;
    CountDownLatch nodeDownSignal = new CountDownLatch(1);
    static ServerSocket serverSocket = null;

    private final String host = "localhost";
    private static final int port = 8000;
    public static final int NUM_DIVISIONS = 10;

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Test zkServer:clientPort");
            return;
        }
        JobTracker t = new JobTracker(args[0]);   
        t.checkpath();

        t.checkIncompleteJobs();

        try{
            serverSocket = new ServerSocket(port);
        }catch(IOException e){
            System.exit(-1);
        }

        while(true){
            try{
                new JobTrackerHandlerThread(serverSocket.accept(), args[0]).start();
            }catch(Exception e){
                System.exit(-1);
            }
        }
    }

    public JobTracker(String hosts) {
        System.out.println("JobTracker Hosts: " + hosts);
        zkc = new ZkConnector();
        try {
            zkc.connect(hosts);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }
 
        watcher = new Watcher() { // Anonymous Watcher
            @Override
            public void process(WatchedEvent event) {
              handleEvent(event);
            } 
        };
    }

    private void checkIncompleteJobs(){
        //create jobs directory in case it doesnt exist
        zkc.create(jobsPath, null, CreateMode.PERSISTENT);

        ZooKeeper zk = zkc.getZooKeeper();
        List<String> children = null;
        try {
            children = zk.getChildren(jobsPath, false);
        }catch(Exception e){
        }

        for (String child : children){
            addIncompleteTasks(child, zk);
        }
    }

    private void addIncompleteTasks(String child, ZooKeeper zk){
        List<String> children = null;
        try {
            children = zk.getChildren(jobsPath + "/" + child, false);
        }catch(Exception e){
        }
        int numMissing = 0;
        if (children == null){
            numMissing = NUM_DIVISIONS;
        }else{
            numMissing = NUM_DIVISIONS - children.size();
        }
        if (numMissing > 0){
            for (int i = children.size() + 1; i <= NUM_DIVISIONS; i++){
              Code ret = zkc.create(
                          jobsPath + "/" + child + "/" + i,         // Path of znode
                          null,           // Data not needed.
                          CreateMode.PERSISTENT   // Znode type, set to EPHEMERAL.
                          );
              System.out.println(ret);
            }
        }
    }
    
    private void checkpath() {
        Stat stat = zkc.exists(jobTrackerPath, watcher);
        String address = host+":"+port;
        if (stat == null) {              // znode doesn't exist; let's try creating it
            Code ret = zkc.create(
                        jobTrackerPath,         // Path of znode
                        address,           // Data not needed.
                        CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
                        );
            zkc.create(jobsPath, null, CreateMode.PERSISTENT);
            if (ret != Code.OK){
                checkpath();
            }
        }else{
            try{
                nodeDownSignal.await();
            }catch(InterruptedException e){
                System.out.println(e.getMessage());
            }
            Code ret = zkc.create(
                        jobTrackerPath,         // Path of znode
                        address,           // Data not needed.
                        CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
                        );
            if (ret != Code.OK){
                nodeDownSignal = new CountDownLatch(1);
                checkpath();
            }
        } 
    }

    private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(jobTrackerPath)) {
            if (type == EventType.NodeDeleted) {
                nodeDownSignal.countDown();
            }
        }
    }
}
