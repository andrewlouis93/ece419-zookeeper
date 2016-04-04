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

	  public WorkerThread(String jobRoute, String zkhost) {
		super("WorkerThread");
		// this.socket = socket;
        System.out.println("jobRoute: " + jobRoute);
        zkc = new ZkConnector();
        try {
          zkc.connect(zkhost);
        } catch(Exception e) {
          System.out.println("Zookeeper connect "+ e.getMessage());
        }
	  }

  	public void run() {
      System.out.println("IN THREAD RUN");
  		// try {
  		// 	/* stream to read from client */
  		// 	ObjectInputStream fromClient = new ObjectInputStream(socket.getInputStream());

  		// 	/* stream to write back to client */
  		// 	ObjectOutputStream toClient = new ObjectOutputStream(socket.getOutputStream());

    //     String packetFromClient = "";

  		// 	while (( packetFromClient = (String) fromClient.readObject()) != null) {
    //              //  String[] msg = packetFromClient.split(":");
    //              //  if (msg[0].equals("newjob")){
    //              //      int ret = addJob(msg[1]);
    //              //      toClient.writeObject("Done");
    //              //      if (ret == 1){
    //              //          divideJobs(msg[1]);
    //              //      }
    //              //  }else if (msg[0].equals("checkstatus")){
    //              //      String status = getStatus(msg[1]);
    //       					  // toClient.writeObject(status);
    //              //  }
  		// 	}
  			
  		// 	/* cleanup when client exits */
  		// 	fromClient.close();
  		// 	toClient.close();
  		// 	socket.close();

  		// } catch (IOException e) {} 
    //     catch (ClassNotFoundException e) {}
  	}

}
