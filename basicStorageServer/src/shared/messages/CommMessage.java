package shared.messages;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

public class CommMessage implements KVMessage {
	
	// Operation in this CommMessage.
	public OptCode opt;
	
	public enum OptCode {
		PUT, /* Put - key value storage */
		GET, /* Get - key value storage */
	}
	
	// Status and data of this CommMessage.
	private StatusType status;
	private String key;
	private String value;

	@Override
	public String getKey() {
		return this.key;
	}

	@Override
	public String getValue() {
		return this.value;
	}

	@Override
	public StatusType getStatus() {
		return this.status;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public void setStatus(StatusType status) {
		this.status = status;
	}
	
	// Serialize a CommMessage into a byte array that represents a JSON string.
	public static byte[] serialize(CommMessage msg) {
		Gson gson = new GsonBuilder().serializeNulls().create();
		return gson.toJson(msg).getBytes();
	}
	
	// Deserialize a CommMessage from given byte array data. Throws Exception
	// if the JSON string embedded in data is not of the correct format.
	public static CommMessage deserialize(byte[] data) throws JsonSyntaxException{
		Gson gson = new GsonBuilder().serializeNulls().create();
		return gson.fromJson(new String(data), CommMessage.class);
	}
}
