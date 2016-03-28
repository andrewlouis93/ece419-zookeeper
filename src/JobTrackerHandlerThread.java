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

public class JobTrackerHandlerThread extends Thread {
	private Socket socket = null;
  private ZkConnector zkc;
  private String jobsPath = "/jobs/";
  private final String JOB_DOESNT_EXIST = "This job doesn't exist";
  private final String NOT_FOUND = "Not found";
  private final String IN_PROGRESS = "Job in progress";

	public JobTrackerHandlerThread(Socket socket, String zkhost) {
		super("JobTrackerHandlerThread");
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
                String[] msg = packetFromClient.split(":");
                if (msg[0].equals("newjob")){
                    int ret = addJob(msg[1]);
                    toClient.writeObject("Done");
                    if (ret == 1){
                        divideJobs(msg[1]);
                    }
                }else if (msg[0].equals("checkstatus")){
                    String status = getStatus(msg[1]);
        					  toClient.writeObject(status);
                }
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
    Stat stat = zkc.exists(jobsPath + hash, null);
    if (stat == null) {              // znode doesn't exist; let's try creating it
        Code ret = zkc.create(
                    jobsPath + hash,         // Path of znode
                    "~:" + JobTracker.NUM_DIVISIONS,           // Data not needed.
                    CreateMode.PERSISTENT   // Znode type, set to EPHEMERAL.
                    );
        return 1;
    }else{
        return 0;
    } 
  }

  private void divideJobs(String hash){
    Stat stat = zkc.exists(jobsPath + hash, null);
    if (stat == null) {              // znode doesn't exist; let's try creating it
        return;
    }else{
      for (int i = 1; i <= JobTracker.NUM_DIVISIONS; i++){
        Code ret = zkc.create(
                    jobsPath + hash + "/" + i,         // Path of znode
                    null,           // Data not needed.
                    CreateMode.PERSISTENT   // Znode type, set to EPHEMERAL.
                    );
        System.out.println(ret);

      }
    } 
  }

  private String getStatus(String hash){
    ZooKeeper zk = zkc.getZooKeeper();
    Stat stat = null;
    try{
        stat = zk.exists(jobsPath + hash, false); 
    }catch(KeeperException e){
        System.out.println(e.code());
        System.exit(-1);
    }catch(Exception e){
        System.exit(-1);
    }

    if (stat==null){
        return JOB_DOESNT_EXIST; 
    }

    byte[] statusBytes = null;
    try {
        statusBytes = zk.getData(jobsPath + hash, false, null);
    } catch(KeeperException e) {
        System.out.println(e.code());
    } catch(Exception e) {
        System.out.println(e.getMessage());
    }

    String status = new String(statusBytes);
    String[] s = status.split(":");
    int tasksComplete = Integer.parseInt(s[1]);
    if (tasksComplete != 0){
        return IN_PROGRESS;
    }else{
        if (s[0].equals("~")){
            return NOT_FOUND;
        }else{
            return s[0];
        }
    }
  }
}
