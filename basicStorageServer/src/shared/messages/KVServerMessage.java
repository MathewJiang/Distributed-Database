/***************************************************************
 * KVServerMessage:
 * 
 * The message type which is only restricted
 * between servers
 * 
 * This type of message is embedded within the CommMessage
 * For client, the KVServerMessage should always be null
 ***************************************************************/
package shared.messages;

public class KVServerMessage {
    public enum KVServerMessageType {
    	PUT,			/* put the message to another server */
    	GET				/* */
    }

    private KVServerMessageType serverMessageType;
    private String key;
    private String value;
    
    public KVServerMessageType getKVServerMessageType() {
    	return serverMessageType;
    }

    public void setKVServerMessageType(KVServerMessageType type) {
    	this.serverMessageType = type;
    }
    
    public String getServerMessageKey() {
    	return key;
    }
    
    public void setServerMessageKey(String key) {
    	this.key = key;
    }
    
    public String getServerMessageValue() {
    	return value;
    }
    
    public void setServerMessageValue(String value) {
    	this.value = value;
    }
}
