package app_kvServer;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import shared.messages.KVAdminMessage;
import shared.metadata.InfraMetadata;
import shared.metadata.ServiceLocation;
import app_kvECS.ECS;
import app_kvServer.storage.Storage;

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
				 * FIXME: need acknowledgment from each server only after that
				 * could the new command be issued
				 */

				if (cm.getKVAdminMessageType() != null) {
					switch (cm.getKVAdminMessageType()) {
					case START:
						
						logger.info("[ZKConnection.java/run()]START!");
						callingServer.setSuspended(false);
						ecs.ack(serverName, "start");
						break;

					case STOP:
						logger.info("[ZKConnection.java/run()]STOP!");
						callingServer.setSuspended(true);
						ecs.ack(serverName, "stop");
						break;

					case SHUTDOWN:
						logger.info("[ZKConnection.java/run()]SHUTDOWN!");

						// Should only contain one backup agent server.
						InfraMetadata shutDownMD = new InfraMetadata();
						String leader = ecs.getLeader();
						
						shutDownMD.getServerLocations().add(callingServer
								.getClusterMD().locationOfService(leader));

						callingServer.setSuspended(true);

						// Metadata still contains this server -> responsible
						// for backing up.
						if (shutDownMD.locationOfService(callingServer
								.getServerName()) != null) {
							// Prepare server for receiving backup messages.
							ecs.waitAckSetup("backupCompleted");
							callingServer.setClusterMD(shutDownMD);
							
							ecs.ack(callingServer.getServerName(), "terminate");
							ecs.waitAck("backupCompleted", 1, 300);
							
							// Flush everything on this leader node.
							try {
								Storage.flush();
							} catch (IOException e) {
								logger.error("Error flushing data to disk on backup leader");
							}

							// At this point the kvdb for this server has become
							// the central repository for backup. Rename kvdb
							// directory for future restore service to pick up.
							callingServer.backupKVDB();
							ecs.ack(callingServer.getServerName(),
									"restoreCompleted");
						} else {
							// This server was not chosen to be the backup
							// agent. Send all of its storage to the restore
							// server.
							callingServer.migrateAll(shutDownMD);
							ecs.ack(callingServer.getServerName(), "terminate");

							// Remove kvdb directory.
							callingServer.removeKVDB();
						}
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
						callingServer.removeKVDB();
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
