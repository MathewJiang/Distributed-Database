package testing;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;

import app_kvECS.ECS;
import app_kvECS.ECSClient;

public class AckTest extends TestCase {

	@Test(timeout=5000000)
	public void testMultiThreadAckWaitAck() {
		ECSClient ecsClientInit = new ECSClient();
		ecsClientInit.initECS();
		ecsClientInit.getECS().reset();
		ecsClientInit.getECS().init();
		List<Thread> threadList = new ArrayList<Thread>();
		ecsClientInit.getECS().waitAckSetup("testack");
		int ackCount = 200;
		for(int i = 0; i < ackCount; i++) {
			final String name = "server_" + i;
			Thread newT = new Thread(new Runnable() {
				@Override
				public void run() {
					ECSClient ecsClient = new ECSClient();
					ecsClient.initECS();
					ECS ecs = ecsClient.getECS();
					
					try {
						ecs.connect("127.0.0.1", 39678);
					} catch (IOException | InterruptedException e) {
						e.printStackTrace();
					}
					System.out.print(name+"\n");
					ecs.ack(name, "testack");
				}
			});
			newT.start();
			threadList.add(newT);
		}
		
		
		
		for(int i = 0; i < threadList.size(); i++) {
			try {
				threadList.get(i).join();
			} catch (InterruptedException e) {
				fail("[AckTest]Thread join failed");
				e.printStackTrace();
			}
		}
		
		ecsClientInit.getECS().waitAck("testack", ackCount, 50);
		
		ecsClientInit.shutdown();
		ecsClientInit.getECS().reset();
		ecsClientInit.getECS().init();
	}

}
