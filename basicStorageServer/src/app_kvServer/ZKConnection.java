package app_kvServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import app_kvECS.ECS;
import shared.messages.CommMessage;

public class ZKConnection implements Runnable {
	private static Logger logger = Logger.getRootLogger();

	private KVServer callingServer;
	private boolean isOpen = false;
	
	public ZKConnection(KVServer callingServer) {
		this.callingServer = callingServer;
		isOpen = true;
	}

	@Override
	public void run() {
		while (isOpen) {
			ECS ecs = callingServer.getECS();
			CommMessage cm = ecs.getCmd(callingServer.getServerName());
			
			if (cm.getAdminMessage() != null) {
				switch (cm.getAdminMessage().getKVAdminMessageType()) {
				case START:
					System.out.println("[ZKConnection.java/run()]START!");
					break;
				
				case STOP:
					System.out.println("[ZKConnection.java/run()]STOP!");
					break;
					
				case SHUTDOWN:
					System.out.println("[ZKConnection.java/run()]SHUTDOWN!");
					callingServer.close();
					isOpen = false;
					break;
					
				case UPDATE:
					System.out.println("[ZKConnection.java/run()]UPDATE!");
					break;
				
				case LOCK_WRITE:
					System.out.println("[ZKConnection.java/run()]LOCK_WRITE!");
					break;
				
				case UNLOCK_WRITE:
					System.out.println("[ZKConnection.java/run()]UNLOCK_WRITE!");
					break;
				
				default:
					logger.error("[ZKConnection.java/run()]Unknown type of AdminMessage");
					break;
				}
			}
		}
	}

	/**
	 * Testing purposes only
	 */
	public static void main(String[] args) {

	}
}
