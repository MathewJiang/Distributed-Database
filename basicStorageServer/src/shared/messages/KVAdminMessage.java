/***************************************************************
 * KVAdminMessage:
 * 
 * The message type which is only restricted
 * between the adminstrator(i.e. ECS) and servers
 * 
 * This type of message is embedded within the CommMessage
 * For client, the KVAdminMessage should always be null
 ***************************************************************/
package shared.messages;

import shared.metadata.InfraMetadata;

public class KVAdminMessage {
    public enum KVAdminMessageType {
    	START,			/* Tell the server to start (accepting client requests) */
    	STOP,			/* Tell the server to stop (not accepting client requests)*/
    	SHUTDOWN, 		/* Tell the server to shut down */
    	UPDATE,			/* Tell the server to update their metadata files
    	 			   	   Servers also need to do necessary migration
    	 			  	   During this period, no clientConnection allowed*/
    	UPDATE_COMPLETE,/* tell the other server that the update has been completed */
    	LOCK_WRITE,		/* Lock the server for write operations */
    	UNLOCK_WRITE,	/* Unlock the server for write operations*/
    	REPORT,			/* Send ack to ECS */
    	SYNC,			/* Make sure every other servers are in the same state */
    	
    	LOCK_WRITE_REMOVE_RECEVIER,	/* During nodeRemove, receiver should know
    	 							   that its successor is
    	 							   the node to be removed*/
    	LOCK_WRITE_REMOVE_SENDER    /* During nodeRemove, sender should know that
    	 							   its predecessor is
    	 							   the node that inherit all its keys
    	 							   (note: itself is the node to be removed) */
    }
    
    public InfraMetadata MD;

    private KVAdminMessageType adminMessageType;
    
    public KVAdminMessageType getKVAdminMessageType() {
    	return adminMessageType;
    }

    public void setKVAdminMessageType(KVAdminMessageType adminMessageType) {
    	this.adminMessageType = adminMessageType;
    }
}
