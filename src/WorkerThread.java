import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.net.*;
import java.io.*;

public class WorkerThread extends Thread {
    private Socket socket = null;
    private ZkConnector zkc;
    private String workerPath = "/worker/";
      
    private final String JOB_DOESNT_EXIST = "This job doesn't exist";
    private final String NOT_FOUND = "Not found";
    private final String IN_PROGRESS = "Job in progress";

	public WorkerThread(Socket socket, String zkhost) {
		super("WorkerThread");
		this.socket = socket;
        zkc = new ZkConnector();
        try {
          zkc.connect(zkhost);
        } catch(Exception e) {
          System.out.println("Zookeeper connect "+ e.getMessage());
        }
	}

	public void run() {
		try {
			/* stream to read from client */
			ObjectInputStream fromClient = new ObjectInputStream(socket.getInputStream());

			/* stream to write back to client */
			ObjectOutputStream toClient = new ObjectOutputStream(socket.getOutputStream());

            String packetFromClient = "";

			while (( packetFromClient = (String) fromClient.readObject()) != null) {
               //  String[] msg = packetFromClient.split(":");
               //  if (msg[0].equals("newjob")){
               //      int ret = addJob(msg[1]);
               //      toClient.writeObject("Done");
               //      if (ret == 1){
               //          divideJobs(msg[1]);
               //      }
               //  }else if (msg[0].equals("checkstatus")){
               //      String status = getStatus(msg[1]);
        					  // toClient.writeObject(status);
               //  }
			}
			
			/* cleanup when client exits */
			fromClient.close();
			toClient.close();
			socket.close();

		} catch (IOException e) {
		} catch (ClassNotFoundException e) {
		}
	}

  private int addJob(String hash){
    Stat stat = zkc.exists(workerPath + hash, null);
    if (stat == null) {              // znode doesn't exist; let's try creating it
        Code ret = zkc.create(
                    workerPath + hash,         // Path of znode
                    "~:" + JobTracker.NUM_DIVISIONS,           // Data not needed.
                    CreateMode.PERSISTENT   // Znode type, set to EPHEMERAL.
                    );
        return 1;
    }else{
        return 0;
    } 
  }
}
