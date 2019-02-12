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

public class KVAdminMessage {
    public enum KVAdminMessageType {
    	START,			/* Tell the server to start (accepting client requests) */
    	STOP,			/* Tell the server to stop (not accepting client requests)*/
    	SHUTDOWN, 		/* Tell the server to shut down */
    	UPDATE,			/* Tell the server to update their metadata files
    	 			   	   Servers also need to do necessary migration
    	 			  	   During this period, no clientConnection allowed*/
    	LOCK_WRITE,		/* Lock the server for write operations */
    	UNLOCK_WRITE	/* Unlock the server for write operations*/
    }

    private KVAdminMessageType adminMessageType;
    
    public KVAdminMessageType getKVAdMessageType() {
    	return adminMessageType;
    }

    public void setKVAdMessageType(KVAdminMessageType adminMessageType) {
    	this.adminMessageType = adminMessageType;
    }
}
