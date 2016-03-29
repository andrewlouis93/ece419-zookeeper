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

/*
    TODO: 
        - Get the path of a sequential node, add the childModified watcher
        - Once the latch is set to 0, make sure it is a new node (NodeChildChanged)
        - And if the node is new, spawn WorkerThread to handle it and reset latch.
*/
        
public class Worker {
    
    String workersPath = "/workers";
    String workerPath = "/worker-";
    ZkConnector zkc;
    Watcher watcher;
    
    CountDownLatch nodeDownSignal = new CountDownLatch(1);
    CountDownLatch childModified = new CountDownLatch(1);
    
    static ServerSocket serverSocket = null;

    private static String host;
    private static int port;

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Worker zkServer:clientPort");
            return;
        }
        Worker t = new Worker(args[0]);  

        try{
            serverSocket = new ServerSocket(0);
            
            host = java.net.InetAddress.getLocalHost().getHostName();
            port = serverSocket.getLocalPort();

        }catch(IOException e){
            System.exit(-1);
        }

        t.checkpath();

        while (true)
            t.checkAssignedJobs();


        // while(true){
        //     try{
        //         // kill worker thread
        //         new WorkerThread(serverSocket.accept(), args[0]).start();
        //     }catch(Exception e){
        //         System.exit(-1);
        //     }
        // }
    }

    public Worker(String hosts) {
        System.out.println("ZK Hosts: " + hosts);
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

    
    private void checkpath() {
        Stat stat = zkc.exists(workersPath, watcher);
        String address = host+":"+port;
        if (stat == null) {              // znode doesn't exist; let's try creating it
            zkc.create(workersPath, null, CreateMode.PERSISTENT);
            Code ret = zkc.create(
                            workersPath + workerPath,         // Path of znode
                            address,                          // Data not needed.
                            CreateMode.EPHEMERAL_SEQUENTIAL  // Znode type, set to EPHEMERAL.
                        );
            
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
                            workersPath + workerPath,         // Path of znode
                            address,                          // Data not needed.
                            CreateMode.EPHEMERAL_SEQUENTIAL   // Znode type, set to EPHEMERAL.
                        );
            if (ret != Code.OK){
                nodeDownSignal = new CountDownLatch(1);
                checkpath();
            }
        } 
    }

    private void checkAssignedJobs() {
        // Stat stat = zkc.exists(workersPath +  watcher);
        // String address = host+":"+port;
        // if (stat == null) {              // znode doesn't exist; let's try creating it
        //     zkc.create(workersPath, null, CreateMode.PERSISTENT);
        //     Code ret = zkc.create(
        //                     workersPath + workerPath,         // Path of znode
        //                     address,                          // Data not needed.
        //                     CreateMode.EPHEMERAL_SEQUENTIAL  // Znode type, set to EPHEMERAL.
        //                 );
        //     if (ret != Code.OK){
        //         checkpath();
        //     }
        // }else{
        //     try{
        //         nodeDownSignal.await();
        //     }catch(InterruptedException e){
        //         System.out.println(e.getMessage());
        //     }
        //     Code ret = zkc.create(
        //                     workersPath + workerPath,         // Path of znode
        //                     address,                          // Data not needed.
        //                     CreateMode.EPHEMERAL_SEQUENTIAL   // Znode type, set to EPHEMERAL.
        //                 );
        //     if (ret != Code.OK){
        //         nodeDownSignal = new CountDownLatch(1);
        //         checkpath();
        //     }
        // } 
    }

    private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(workersPath)) {
            if (type == EventType.NodeDeleted) {
                nodeDownSignal.countDown();
            }
            else if(type == EventType.NodeChildrenChanged){

            }
        }
    }
}
