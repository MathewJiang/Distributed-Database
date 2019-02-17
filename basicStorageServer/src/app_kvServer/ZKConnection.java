package app_kvServer;

import org.apache.log4j.Logger;

import shared.InfraMetadata;
import shared.messages.KVAdminMessage;
import app_kvECS.ECS;

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
			KVAdminMessage cm = ecs.getCmd(callingServer.getServerName());

			if (cm != null) {

				/*
				 * FIXME: need acknowledgement from each server only after that
				 * could the new command be issued
				 */

				switch (cm.getKVAdminMessageType()) {
				case START:
					logger.info("[ZKConnection.java/run()]START!");
					// TODO
					callingServer.setSuspended(false);
					break;

				case STOP:
					logger.info("[ZKConnection.java/run()]STOP!");
					callingServer.setSuspended(true);
					break;

				case SHUTDOWN:
					logger.info("[ZKConnection.java/run()]SHUTDOWN!");
					callingServer.close();
					isOpen = false;
					break;

				case UPDATE:
					logger.info("[ZKConnection.java/run()]UPDATE!");
					callingServer.setSuspended(true);
					// 1. Try geting lock
					InfraMetadata newMD = ecs.getMD();
					try {
						callingServer.migrateWithNewMD(newMD);
					} catch (Exception e) {
						logger.error("Error migrating data on server " + callingServer.getServerInfo());
						e.printStackTrace();
					}
					// 2. Ack ecs (non-blocking).
					break;

				case UPDATE_COMPLETE:
					logger.info("[ZKConnection.java/run()]UPDATE_COMPLETE!");
					callingServer.setSuspended(false);
					break;

				case LOCK_WRITE:
					logger.info("[ZKConnection.java/run()]LOCK_WRITE!");
					break;

				case UNLOCK_WRITE:
					logger.info("[ZKConnection.java/run()]UNLOCK_WRITE!");
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
