import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class JobTracker {
    
    String jobTrackerPath = "/jobtracker";
    ZkConnector zkc;
    Watcher watcher;
    String address = "test";
    CountDownLatch nodeDownSignal = new CountDownLatch(1);

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Test zkServer:clientPort");
            return;
        }
        JobTracker t = new JobTracker(args[0]);   
        t.checkpath();

        while(true){
            System.out.println("TEST");
            try{
            Thread.sleep(1000);

            }catch(Exception e){
              
            }

        }
    }

    public JobTracker(String hosts) {
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
        Stat stat = zkc.exists(jobTrackerPath, watcher);
        if (stat == null) {              // znode doesn't exist; let's try creating it
            Code ret = zkc.create(
                        jobTrackerPath,         // Path of znode
                        address,           // Data not needed.
                        CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
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
