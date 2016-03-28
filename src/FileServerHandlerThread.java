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

public class FileServerHandlerThread extends Thread {
	private Socket socket = null;
    private ZkConnector zkc;

	public FileServerHandlerThread(Socket socket, String zkhost) {
		super("FileServerHandlerThread");
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

            String packetFromClient = (String)fromClient.readObject();
            assert( packetFromClient != null );
			
            // Worker sends Partition ID
            // FileServer sends relevant Dict.

			/* cleanup when client exits */
			fromClient.close();
			toClient.close();
			socket.close();

		} catch (IOException e) {
		} catch (ClassNotFoundException e) {
		}
	}

}
