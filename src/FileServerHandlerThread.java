import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.util.List;
import java.util.ArrayList;

import java.net.*;
import java.io.*;

public class FileServerHandlerThread extends Thread {
	private Socket socket = null;
    private ZkConnector zkc;
    private List dictionary = null;

	public FileServerHandlerThread(Socket socket, String zkhost, List dictionary) {
		super("FileServerHandlerThread");
		this.socket = socket;
        this.dictionary = dictionary;

        zkc = new ZkConnector();
        
        try {
          zkc.connect(zkhost);
        } catch(Exception e) {
          System.out.println("Zookeeper connect "+ e.getMessage());
        }
	}

    // parition idx = 1... JobTracker.NUM_DIVISIONS (10)
    public List getDictionary(int partition_id){
        assert(partition_id <= JobTracker.NUM_DIVISIONS);
        int partition_size = dictionary.size()/JobTracker.NUM_DIVISIONS;
        int start_index = (partition_id - 1) * partition_size;
        return new ArrayList(dictionary.subList( start_index, partition_size ));
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
