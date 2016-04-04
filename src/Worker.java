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
import java.util.Random;
import java.net.*;
import java.io.*;

/*
    TODO: 
        - Get the path of a sequential node, add the childModified watcher
        - Once the latch is set to 0, make sure it is a new node (NodeChildChanged)
        - And if the node is new, spawn WorkerThread to handle it and reset latch.
*/
        
public class Worker {

    String jobsPath = "/jobs";
    String workersPath = "/workers";
    String workerPath = "/worker-";
    
    String fsHost = null;
    Integer fsPort = null;

    ZkConnector zkc;
    Watcher watcher;


    // static ServerSocket serverSocket = null;

    private static String host;
    private static int port;
    private static String zkcHosts;

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
                // handleEvent(event);
            }
        };
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Worker zkServer:clientPort");
            return;
        }
        zkcHosts = args[0];
        Worker t = new Worker(args[0]);

        t.registerFileServer();
        t.registerWorker();
        t.initializeWatcher();

        
        while (true){
            try{
                t.processJobs();    
            }catch(KeeperException e){
                System.out.println(e);
            }catch(InterruptedException e){
                System.out.println(e);
            }catch(IOException e){
                System.out.println(e);
            }

        }
    }

    private void processJobs() throws KeeperException, InterruptedException, IOException{
        List<String> pendingTasks = new ArrayList<String>();
        List<String> allTasks = zkc.zooKeeper.getChildren(jobsPath, false);

        for (String task: allTasks){
            byte[] dataBytes = zkc.zooKeeper.getData(
                jobsPath + "/" + task, 
                false, 
                null
            );
            String numJobs = new String(dataBytes);

            if (!numJobs.equals("null")){
                String[] s = numJobs.split(":");
                
                int tasksRemaining = Integer.parseInt(s[1]);
                if (tasksRemaining > 0){
                    pendingTasks.add( task );                            
                }
            }
        }

        if (pendingTasks.size() > 0){
            Random randomGenerator = new Random();                    
            // select job from a task.
            int index = randomGenerator.nextInt( pendingTasks.size() );
            String selectedTask = pendingTasks.get(index);
            // select job from task (task => hash, job => partition)
            List<String> allJobs = zkc.zooKeeper.getChildren( jobsPath + "/" + selectedTask , false);

            if (allJobs.size() > 0){
                index = randomGenerator.nextInt( allJobs.size() );
                String selectedJob = allJobs.get(index);
                System.out.println("Processing: " + selectedTask + "/" + selectedJob);
                

                Socket dictSock = new Socket(fsHost, fsPort);
                System.out.println(fsPort);

                ObjectOutputStream toFS = new ObjectOutputStream(dictSock.getOutputStream());
                toFS.writeObject(selectedJob);
                ObjectInputStream fromFS = new ObjectInputStream(dictSock.getInputStream());

                List<String> dictPartition;
                try{
                    dictPartition = (List)fromFS.readObject();    
                }catch(Exception e){
                    System.out.println(e);
                }

                fromFS.close();
                toFS.close();
                dictSock.close();

                
            }
            else{
                System.out.println("allJobs size 0");
            }

        }else{
            System.out.println("pendingTasks size 0");
        }
    }

    private void registerWorker(){
        Stat stat = zkc.exists( workersPath, null );
        // /workers hasn't been initialized yet.
        if (stat == null){
            zkc.create( workersPath, null, CreateMode.PERSISTENT );
        }
        Code ret = zkc.create(
            workersPath + workerPath,
            null,
            CreateMode.EPHEMERAL_SEQUENTIAL
        );
        if (ret != Code.OK){
            System.out.println("???");
        }
    }

    private void registerFileServer(){
        Stat stat = zkc.exists( "/fileserver", null );

        if (stat == null){}
        else{
            byte[] dataBytes;
            try{
                dataBytes = zkc.zooKeeper.getData(
                    "/fileserver",
                    false,
                    null
                );
                String fsLocation = new String(dataBytes);
                String[] s = fsLocation.split(":");

                fsHost = s[0];
                fsPort = Integer.valueOf(s[1]);
            }catch(KeeperException e){
                System.out.println(e);
            }catch(InterruptedException e){
                System.out.println(e);
            }
        }
    }

    private void initializeWatcher() {
        try{
            zkc.zooKeeper.getChildren(jobsPath, watcher);
        }
        catch(KeeperException e){
            System.out.println(e);
        }
        catch(InterruptedException e){
            System.out.println(e);
        }
    }


    // private void handleEvent(WatchedEvent event) {
    //     ZooKeeper zk = zkc.getZooKeeper();        
    //     String path = event.getPath();
    //     System.out.println("Worker Watcher got path: " + path);
    //     System.out.println("Event Type: " + event.getType());
    //     EventType type = event.getType();
    //     if(path.equalsIgnoreCase(jobsPath)) {
    //         if (type == EventType.NodeDeleted) {
    //             // nodeDownSignal.countDown();
    //         }
    //         else if(type == EventType.NodeChildrenChanged){
    //             try{

    //                 processJobs();
    //                 // get list of jobs, select a job, and
    //                 jobsAvailable.countDown();                    

    //             }catch(KeeperException e){
    //                 System.out.println(e);
    //             }
    //             catch(InterruptedException e){
    //                 System.out.println(e);
    //             }
    //             catch(IOException e){
    //                 System.out.println(e);
    //             }
    //         }
    //     }
    // }

}
