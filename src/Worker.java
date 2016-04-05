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
import java.security.MessageDigest;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.net.*;
import java.io.*;
 
public class Worker {
 
    String jobsPath = "/jobs";
    String workersPath = "/workers";
    String workerPath = "/worker-";
 
    String fsHost = null;
    Integer fsPort = null;
 
    ZkConnector zkc;
    CountDownLatch nodeUpSignal = new CountDownLatch(1);
    Watcher watcher;
 
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
                handleEvent(event);
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
 
        // t.registerFileServer();
        t.registerWorker();        
 
        while (true){
            try{
                System.out.println("before register");
                t.registerFileServer();
                System.out.println("after register");
                t.processJobs();    
 
                // sleep to reduce prob. of overlapping tasks being done.
                // Thread.currentThread().sleep(10);
                Thread.currentThread().sleep(100); 
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
            // System.out.println("numJobs: " + numJobs);
            if (!numJobs.equals("null")){
                String[] s = numJobs.split(":");
 
                int tasksRemaining = Integer.parseInt(s[1]);
                if ( (tasksRemaining > 0) && (s[0].equals("~")) ){
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
                ObjectOutputStream toFS = new ObjectOutputStream(dictSock.getOutputStream());
                ObjectInputStream fromFS = new ObjectInputStream(dictSock.getInputStream());
 
                System.out.println ("SENT REQ: " + selectedJob);
                toFS.writeObject(selectedJob);
                toFS.flush();
 
                List<String> dictPartition = null;
                try{
                    dictPartition = (List)fromFS.readObject();    
                }catch(Exception e){
                    System.out.println(e);
                }
 
                if (dictPartition != null)
                    System.out.println("RECEIVED DICT: " + selectedJob);
 
                fromFS.close();
                toFS.close();
                dictSock.close();
 
                String pword = findPassword(dictPartition, selectedTask);
                System.out.println("PASSWORD IS : " + pword);
 
                // decremenet job, and delete node.
                updateZookeeper( selectedTask, selectedJob, pword );
 
            }
            else{
                System.out.println("No Pending Jobs Left.");
            }
 
        }else{
            System.out.println("No Pending Tasks Left.");
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
        Stat stat = zkc.exists( "/fileserver", watcher );
 
        if (stat == null){
            // wait until fileserver exists
            try{
                nodeUpSignal.await();    
            }catch(InterruptedException e){
                e.printStackTrace();
            }
 
        }
 
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
 
    private String convertToMD5(String src){
        try{
            MessageDigest md = MessageDigest.getInstance("MD5");    
 
            md.update(src.getBytes());
            byte byteData[] = md.digest();
            //convert the byte to hex format method 1
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < byteData.length; i++) {
             sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
            }
            //convert the byte to hex format method 2
            StringBuffer hexString = new StringBuffer();
            for (int i=0;i<byteData.length;i++) {
                String hex=Integer.toHexString(0xff & byteData[i]);
                if(hex.length()==1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();
        }catch(Exception e){
            System.out.println(e);
        }
        return null;
    }
 
    private String findPassword(List<String> passwords, String passwordHash){
        for (String password : passwords){
            if ( convertToMD5(password).equals(passwordHash) ){
                return password;
            }
        }
        return null;
    }
 
    // decrement node in jobs/x
    // delete jobs/task/job
    // if password is found, ammend data. 
    private void updateZookeeper(String task, String job, String result) {
 
        try{
            ZooKeeper zk = zkc.getZooKeeper();
            // decrement node
            byte[] dataBytes = zk.getData(
                "/jobs/" + task, 
                false, 
                null
            );
            String numJobs = new String(dataBytes);
            String[] s = numJobs.split(":");
            int _numJobs = Integer.parseInt(s[1]);
 
            String tasksRemaining;
            // amend data w/ password if found.
            if (result != null && s[0].equals("~")){
                tasksRemaining = (result + ":" + String.valueOf(_numJobs));
            }
 
            // if there's a password by this time
            else if (!s[0].equals("~")){
                tasksRemaining = numJobs;
            }
            else{
                tasksRemaining = ("~:" +  String.valueOf(_numJobs - 1));
            }
 
            System.out.println("tasks remaining: " + tasksRemaining);
            String _data = tasksRemaining;
            Stat stat = zk.setData(
                    "/jobs/" + task, 
                    _data.getBytes(), 
                    -1
                );
            if (stat == null){
                System.out.print("Job /jobs/" + task + " was completed while I was working.");
            }
 
 
            // delete the node, if it exists.
            try{
                String toDelete = "/jobs/" + task + "/" + job;
                Stat _s = zkc.exists( toDelete, null );
                // /workers hasn't been initialized yet.
                if (_s != null){
                    zk.delete(toDelete, -1);    
                }
            }catch(KeeperException e){
                System.out.print(e);
                System.out.println("Attempting to Delete Node");
            }
 
 
        }catch(KeeperException e){
            e.printStackTrace();
        }catch(InterruptedException e){
            e.printStackTrace();
        }
 
    }
 
 
 
 
    private void handleEvent(WatchedEvent event) {
        ZooKeeper zk = zkc.getZooKeeper();        
        String path = event.getPath();
        System.out.println("Worker Watcher got path: " + path);
        System.out.println("Event Type: " + event.getType());
        EventType type = event.getType();
        if(path.equalsIgnoreCase("/fileserver")) {
            if (type == EventType.NodeCreated) {
                nodeUpSignal.countDown();
                registerFileServer();                
            }
            else if (type == EventType.NodeDeleted){
                System.out.println("FILESERVER DELETED");
                nodeUpSignal = new CountDownLatch(1);
            }
 
        }
    }
 
}
