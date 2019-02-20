package app_kvServer;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.log4j.Logger;

import shared.InfraMetadata;
import shared.InfraMetadata.ServiceLocation;
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
		ECS ecs = callingServer.getECS();
		String serverName = callingServer.getServerName();

		ecs.ack(serverName, "launched");
		while (isOpen) {
			KVAdminMessage cm = ecs.getCmd(serverName);
			logger.info("[ZKConnection.java]AdminMessage: "
					+ cm.getKVAdminMessageType());

			if (cm != null) {
				/*
				 * FIXME: need acknowledgement from each server only after that
				 * could the new command be issued
				 */

				if (cm.getKVAdminMessageType() != null) {
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
						callingServer.setShuttingDown(true);
						callingServer.close();
						isOpen = false;
						break;

					case LOCK_WRITE:
						logger.info("[ZKConnection.java/run()]UPDATE!");
						callingServer.setWriteLock(true);

						// Await ECS to finish updating metadata.
						ecs.lock();
						ecs.unlock();

						// 1. Does not replace current metadata.
						// 2. Only sending copies of migrating keys.
						callingServer.migrateWithNewMD(ecs.getMD());

						ecs.ack(callingServer.getServerName(), "migrate");
						break;

					case UPDATE_COMPLETE:
						logger.info("[ZKConnection.java/run()]UPDATE_COMPLETE!");
						callingServer.setSuspended(false);
						break;

					case SYNC:
						callingServer.removeMigratedKeys(ecs.getMD());
						callingServer.setWriteLock(false);
						callingServer.setSuspended(false);

						ecs.ack(callingServer.getServerName(), "sync");
						break;

					case REPORT:
						ecs.ack(callingServer.getServerName(), "report");
						break;

					case LOCK_WRITE_REMOVE_RECEVIER:
						try {
							// Remove the "fading" server from own server
							// locations MD and use the MD to reset hash ring.
							callingServer
									.getClusterMD()
									.getServerLocations()
									.remove(callingServer.getClusterHash()
											.getPredeccessor(
													callingServer
															.getServerInfo()));
							callingServer.setClusterMD(callingServer
									.getClusterMD());
						} catch (Exception e) {
							logger.error("Error reseting metadata: " + e);
						}

						// New metadata has been computed without the fading
						// server. Messages from that server will now be
						// accepted. Acknowledging ECS to issue migration.
						ecs.ack(callingServer.getServerName(), "computedNewMD");
						break;

					// Removing this server from cluster.
					case LOCK_WRITE_REMOVE_SENDER:
						// Compute new metadata without the server itself.
						InfraMetadata newMD = callingServer.getClusterMD()
								.duplicate();
						newMD.removeServerLocation(logger,
								callingServer.getServerName());

						// Send and remove all keys in the current server.
						callingServer.migrateAll(newMD);
						
						ecs.lock();
						ecs.unlock();
						
						// Ack ECS after migration completes and commits
						// suicide.
						ecs.ack(callingServer.getServerName(), "migrate");
						callingServer.setShuttingDown(true);
						callingServer.close();
						isOpen = false;
						break;

					default:
						logger.error("[ZKConnection.java/run()]Unknown type of AdminMessage");
						break;
					}
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
