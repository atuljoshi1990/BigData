import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class PrinterMaster implements Watcher, Runnable {

	static boolean interrupt = false;
	private static final int SESSION_TIMEOUT = 5000;
	private ZooKeeper zk;
	private CountDownLatch connectedSignal = new CountDownLatch(1);

	public void connect(String hosts) throws IOException, InterruptedException {
		zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
		connectedSignal.await();
	}

	@Override
	public void process(WatchedEvent event) { // Watcher interface
		if (event.getState() == KeeperState.SyncConnected) {
			connectedSignal.countDown();
		}
	}
	//create znode.
	public void create(String groupName) throws KeeperException,
			InterruptedException {
		String path = "/" + groupName;
		String createdPath = zk.create(path, null/* data */,
				Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println("Created " + createdPath);
	}

	public void close() throws InterruptedException {
		zk.close();
	}
	//delete the children once the printer processing is done.
	public void delete(String groupName, String coolOffPeriod)
			throws KeeperException, InterruptedException,
			UnsupportedEncodingException {
		String path = "/" + groupName;

		try {
			List<String> children = zk.getChildren(path, false);
			for (String child : children) {
				String childData = new String(zk.getData(path + "/" + child,
						false, null), "UTF-8");
				System.out.println("Data printed : " + childData
						+ " for client : " + child);
				zk.delete(path + "/" + child, -1);
				Thread.sleep(Long.parseLong(coolOffPeriod));
			}
		} catch (KeeperException.NoNodeException e) {
			System.out.printf("Group %s does not exist\n", groupName);
			System.exit(1);
		}
	}

	public static void main(String[] args) throws Exception {
		Thread thread = new Thread(new PrinterMaster());
		thread.start();
		PrinterMaster create = new PrinterMaster();
		PrinterMaster delete = new PrinterMaster();
		create.connect(args[0]); 
		try {
			create.create(args[1]);
		} catch (Exception exe) {
			System.out.println("Master Node already present");
		}
		create.close();
		delete.connect(args[0]);
		while (true) {
			delete.delete(args[1], args[2]);
			if (interrupt) {
				//terminate if exit is entered on the console.
				System.out.println("Program ends.");
				break;
			}
		}
	}
	
	//code to implement exit functionality
	@Override
	public void run() {
		while (true) {
			@SuppressWarnings("resource")
			Scanner sc = new Scanner(System.in);
			if ("exit".equalsIgnoreCase(sc.nextLine())) {
				interrupt = true;
				break;
			}
		}
	}
}
