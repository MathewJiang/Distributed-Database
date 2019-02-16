package app_kvServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.log4j.Logger;

import app_kvECS.ECS;
import shared.InfraMetadata;
import shared.InfraMetadata.ServiceLocation;
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
				
				/*
				 * FIXME: need acknowledgement from each server
				 * only after that could the new command be issued
				 */
				
				switch (cm.getAdminMessage().getKVAdminMessageType()) {
				case START:
					System.out.println("[ZKConnection.java/run()]START!");
					//TODO 
					callingServer.setSuspended(false);
					break;
				
				case STOP:
					System.out.println("[ZKConnection.java/run()]STOP!");
					callingServer.setSuspended(true);
					break;
					
				case SHUTDOWN:
					System.out.println("[ZKConnection.java/run()]SHUTDOWN!");
					callingServer.close();
					isOpen = false;
					break;
					
				case UPDATE:
					System.out.println("[ZKConnection.java/run()]UPDATE!");
					callingServer.setSuspended(true);
					
					getChangedNode(callingServer.getClusterMD(), cm.getInfraMetadata());
					break;
				
				case UPDATE_COMPLETE:
					System.out.println("[ZKConnection.java/run()]UPDATE_COMPLETE!");
					callingServer.setSuspended(false);
					callingServer.setClusterMD(cm.getInfraMetadata());
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

	
	/****************************************************************************
	 * getChangedNode
	 * Get node that has been added/removed from the system
	 * 
	 * @return	corresponding ServiceLocation
	 * 			null if no change
	 * 
	 * !!!!!FIXME: need to change to "private"
	 ****************************************************************************/
	public ServiceLocation getChangedNode(InfraMetadata oldMetaData, InfraMetadata newMetaData) {
		ArrayList<ServiceLocation> oldServiceLocations = (ArrayList)oldMetaData.getServerLocations();
		ArrayList<ServiceLocation> newServiceLocations = (ArrayList)newMetaData.getServerLocations();
		ServiceLocation newNode = null;
		
		// determine whether adding or removing node
		if (oldServiceLocations.size() == newServiceLocations.size()) {
			return null;
		} else if (oldServiceLocations.size() < newServiceLocations.size()) {
			// detect a new node being added
			HashSet<String> oldServiceLocationsSet = new HashSet<String>();
			for (ServiceLocation sl : oldServiceLocations) {
				oldServiceLocationsSet.add(sl.serviceName);
			}
			
			for (ServiceLocation sl : newServiceLocations) {
				if (!oldServiceLocationsSet.contains(sl.serviceName)) {
					newNode = sl;
					break;
				}
			}
			logger.info("[ClienConnection.java/willMigrate]New: " + newNode.serviceName + " has been added!");
			System.out.println("[debug]ClienConnection.java/willMigrate: New: " + newNode.serviceName + " has been added!");
		} else {
			// detect a node being removed
			HashSet<String> newServiceLocationsSet = new HashSet<String>();
			
			for (ServiceLocation sl : newServiceLocations) {
				newServiceLocationsSet.add(sl.serviceName);
			}
			
			for (ServiceLocation sl : oldServiceLocations) {
				if (!newServiceLocationsSet.contains(sl.serviceName)) {
					newNode = sl;
					break;
				}
			}
			logger.info("[ClienConnection.java/willMigrate]Node: " + newNode.serviceName + " has been removed!");
			System.out.println("[debug]ClienConnection.java/willMigrate: Node: " + newNode.serviceName + " has been removed!");
		}
		
		return newNode;
	}
	
	
	/**
	 * Testing purposes only
	 */
	public static void main(String[] args) {

	}
}
