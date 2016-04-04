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
    public List getDictionary(String _partition_id){
    	int partition_id = Integer.parseInt(_partition_id);
        assert(partition_id <= JobTracker.NUM_DIVISIONS);
        int partition_size = dictionary.size()/JobTracker.NUM_DIVISIONS;
        int start_index = (partition_id - 1) * partition_size;
        return new ArrayList(dictionary.subList( start_index, start_index + partition_size ));
    }

	public void run() {
		try {
			/* stream to read from client */
			ObjectOutputStream toClient = new ObjectOutputStream(socket.getOutputStream());
			/* stream to write back to client */
			ObjectInputStream fromClient = new ObjectInputStream(socket.getInputStream());			

			String packetFromClient = (String)fromClient.readObject();
			
			System.out.println("CLI REQUEST: " + packetFromClient);
			List<String> toSend = getDictionary(packetFromClient);
			
			toClient.writeObject(toSend);
			System.out.println("SENT: " + packetFromClient);

			/* cleanup when client exits */
			toClient.close();
			fromClient.close();
			socket.close();
			zkc.close();

		} catch (IOException e) {
			System.out.println("FileServerHandlerThread");
			e.printStackTrace();
		}
		catch (ClassNotFoundException e) {
			System.out.println(e);
		}
		catch (InterruptedException e){
			e.printStackTrace();
		}
	}

}
