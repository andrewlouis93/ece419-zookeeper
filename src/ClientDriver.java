import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import java.util.concurrent.CountDownLatch;

import java.net.*;
import java.io.*;

public class ClientDriver implements Runnable {

    private String jobTrackerPath = "/jobtracker";
    private String jobPath = "/jobs/";
    private CountDownLatch nodeCreatedSignal = new CountDownLatch(1);
    private ZooKeeper zk;
    private int jobOrStatus;
    private String jobId;
    private Thread runningThread;

    private Socket socket = null;
    private ObjectOutputStream out = null;
    private ObjectInputStream in = null;
    
    public static void main(String[] args) {
  
        if (args.length != 3) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. B zkServer:clientPort");
            return;
        }
    
        ZkConnector zkc = new ZkConnector();
        try {
            zkc.connect(args[0]);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

        int job;
        if (args[1].equals("job")){
            job = 0;
        }else{
            job = 1;
        }

        ClientDriver cd = new ClientDriver(job, args[2], zkc);
        cd.run();
        try{
          zkc.close();
        }catch(InterruptedException e){

        }
    }

    public ClientDriver(int jobOrStatus, String jobId, ZkConnector zkc){
        this.jobOrStatus = jobOrStatus; 
        this.jobId = jobId;
        zk = zkc.getZooKeeper();
    }

    public void run(){
        try {
            runningThread = Thread.currentThread();
            Stat stat = null;
            try{
                stat = zk.exists(jobTrackerPath, new Watcher(){
                    @Override
                    public void process(WatchedEvent event){
                        handleEvent(event);
                    }
                });
            }catch(KeeperException e){
                System.out.println(e.code());
                runningThread.interrupt();
            }

            if (stat==null){
                System.out.println("Waiting for node to be created");
                nodeCreatedSignal.await();
            }

            byte[] addrBytes = null;
            try {
                addrBytes = zk.getData(jobTrackerPath, false, null);
            } catch(KeeperException e) {
                System.out.println(e.code());
            } catch(Exception e) {
                System.out.println(e.getMessage());
            }

            String addr = new String(addrBytes);
            connectToJobTracker(addr);
        }catch(InterruptedException e){
            System.out.println("interrupted");
            nodeCreatedSignal = new CountDownLatch(1);
            this.run();
        }
    }

    private void connectToJobTracker(String addr){
        String[] hostport = addr.split(":");
        String host = hostport[0];
        int port = Integer.parseInt(hostport[1]);

        while(true){
            try{
                socket = new Socket(host, port);
                out = new ObjectOutputStream(socket.getOutputStream());
                in = new ObjectInputStream(socket.getInputStream());
                break;
            }catch(ConnectException e){
              try{
                Thread.sleep(500);
              }catch(InterruptedException ex){
              }
            }catch(Exception e){
                runningThread.interrupt();
            }
        }

        System.out.println(socket);

        if(jobOrStatus == 0){
            addJob();
        }else if (jobOrStatus == 1){
            checkStatus();
        }
    }

    private void addJob(){
        String toJobTracker = "newjob:" + jobId;
        try{
            out.writeObject(toJobTracker);
            String reply = (String) in.readObject();
        }catch(Exception e){
            e.printStackTrace();
            runningThread.interrupt();
        }
        
    }

    private void checkStatus(){
        System.out.println("Check Status");
        String toJobTracker = "checkstatus:" + jobId;
        try{
            out.writeObject(toJobTracker);
            String reply = (String) in.readObject();
            System.out.println(reply);
        }catch(Exception e){
            runningThread.interrupt();
        }

    }


    private void handleEvent(WatchedEvent event){
        boolean isNodeCreated = event.getType().equals(EventType.NodeCreated);
        boolean isNodeFailed = event.getType().equals(EventType.NodeDeleted);
        boolean isMyPath = event.getPath().equals(jobTrackerPath);
        if (isNodeCreated && isMyPath) {
            try{
                zk.exists(jobTrackerPath, new Watcher(){
                    @Override
                    public void process(WatchedEvent event){
                        handleEvent(event);
                    }
                });
            }catch(KeeperException e){
                System.out.println(e.code());
            }catch(Exception e){
                System.out.println(e.getMessage());
            }
            nodeCreatedSignal.countDown();
        }
        else if (isNodeFailed && isMyPath){
            runningThread.interrupt();
        }
    }
}
