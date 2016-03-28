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
import java.util.ArrayList;
import java.util.Scanner;

import java.net.*;
import java.io.*;

public class FileServer {
    
    String fileServerPath = "/fileserver";
    
    ZkConnector zkc;
    Watcher watcher;
    List dictionary = null;
    CountDownLatch nodeDownSignal = new CountDownLatch(1);

    static ServerSocket serverSocket = null;

    private final String host = "localhost";
    private static final int port = 8001;
    


    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: java -classpath ../src/lib/zookeeper-3.3.2.jar:../src/lib/log4j-1.2.15.jar:../src/.  FileServer $1:$2 ../src/dictionary/lowercase.rand");
            return;
        }
        FileServer t = new FileServer(args[0], args[1]); 
        t.checkpath();

        try{
            serverSocket = new ServerSocket(port);
        }catch(IOException e){
            System.exit(-1);
        }

        while(true){
            try{
                new FileServerHandlerThread(
                        serverSocket.accept(), 
                        args[0]
                    ).start();
            }catch(Exception e){
                System.exit(-1);
            }
        }
    }

    public FileServer(String hosts, String dictPath) {
        System.out.println("Dict Path: " + dictPath);
        System.out.println("NUM DIVISIONS: " + JobTracker.NUM_DIVISIONS);
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
        
        dictionary = initializeDictionary(dictPath);
        // getDictionary(1);
    }
    
    private void checkpath() {
        Stat stat = zkc.exists(fileServerPath, watcher);
        String address = host+":"+port;
        if (stat == null) {              // znode doesn't exist; let's try creating it
            Code ret = zkc.create(
                            fileServerPath,         // Path of znode
                            address,                // Data not needed.
                            CreateMode.EPHEMERAL    // Znode type, set to EPHEMERAL.
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
            
            // reset service, and reinitiate latch
            // if primary goes down.
            Code ret = zkc.create(
                            fileServerPath,         // Path of znode
                            address,                // Data not needed.
                            CreateMode.EPHEMERAL    // Znode type, set to EPHEMERAL.
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
        if(path.equalsIgnoreCase(fileServerPath)) {
            if (type == EventType.NodeDeleted) {
                nodeDownSignal.countDown();
            }
        }
    }

    private static List initializeDictionary(String filePath){
        ArrayList dict = new ArrayList();

        File dictFile = new File(filePath); 
        Scanner fileScan;
        
        try{
            fileScan = new Scanner(dictFile);
            while(fileScan.hasNextLine()){
                dict.add(fileScan.nextLine());
            }
            fileScan.close();            
        }
        catch(IOException e){
            e.printStackTrace();
        }
        
        return dict;
    }
    
    // parition idx = 1... JobTracker.NUM_DIVISIONS (10)
    public List getDictionary(int partition_id){
        assert(partition_id <= JobTracker.NUM_DIVISIONS);
        int partition_size = dictionary.size()/JobTracker.NUM_DIVISIONS;
        int start_index = (partition_id - 1) * partition_size;
        return new ArrayList(dictionary.subList( start_index, partition_size ));
    }
}
