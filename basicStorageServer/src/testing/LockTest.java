package testing;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import app_kvECS.ECS;
import app_kvECS.ECSClient;

public class LockTest {

	private void testLoop(ECS ecs) {
		for(int i = 0; i < 2000; i++) {
			ecs.lock();
			ecs.unlock();
		}
	}
	@Test(timeout=10000)
	public void testSingleThreadLockUnlock() {
		ECS ecs = new ECS();
		try {
			ecs.connect("127.0.0.1", 39678);
			ecs.reset();
			ecs.init();
			testLoop(ecs);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			fail("failed to connect ECS");
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			fail("failed to connect ECS");
		}
		//
	}
	@Test(timeout=5000000)
	public void testMultiThreadLockUnlock() {
		ECSClient ecsClientInit = new ECSClient();
		ecsClientInit.initECS();
		ecsClientInit.getECS().reset();
		ecsClientInit.getECS().init();
		List<Thread> threadList = new ArrayList<Thread>();
		for(int i = 0; i < 8; i++) {
			
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
					testLoop(ecs);
				}
			});
			newT.start();
			threadList.add(newT);
		}
		for(int i = 0; i < threadList.size(); i++) {
			try {
				threadList.get(i).join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		ecsClientInit.shutdown();
		ecsClientInit.getECS().reset();
	}

}
