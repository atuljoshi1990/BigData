import java.nio.charset.Charset;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

public class PrinterClient extends ConnectionWatcher {
	public void join(String groupName, String memberName, String data,
			String timOutPeriod) throws KeeperException, InterruptedException {
		String path = "/" + groupName + "/" + memberName;
		byte dataArray[] = data.getBytes(Charset.forName("UTF-8"));
		String createdPath = zk.create(path, dataArray, Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL);
		long previousTime = System.currentTimeMillis();
		System.out.println("Created " + createdPath);
		while (null != zk.exists(createdPath, false)) {
			long now = System.currentTimeMillis();
			if ((now - previousTime) > Long.parseLong(timOutPeriod)) {
				//whenever the master is no longer accepting any request give error.
				System.out
						.println("Master must be swamped/it should have failed. Please report this to the admin.");
				System.exit(0);
			}
		}
		//if the master has printed out the job and deleted it give the success message.
		System.out.println("Your print job is processed, check the printer.");
		System.exit(0);
	}

	public static void main(String[] args) throws Exception {
		PrinterClient client = new PrinterClient();
		client.connect(args[0]); // 
		try{
			client.join(args[1], args[2], args[3], args[4]); 
		}catch(Exception exe){
			System.out.println("Master node not created.");
		}
		// stay alive until process is killed or thread is interrupted
		Thread.sleep(Long.MAX_VALUE);
	}
}
