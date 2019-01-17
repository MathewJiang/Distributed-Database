package shared.messages;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

public class CommMessage {
	// CONNECT/DISCONNECT requests should come with an empty message field.
	public enum OptCode {
		CONNECT, /* Connect - session */
		DISCONNECT, /* Disconnect - session */
		PUT, /* Put - key value storage */
		GET, /* Get - key value storage */
	}
	
	// States specific to CONNECT requests.
	public enum OptState {
		CONNECT_SUCCESS,
		CONNECT_FAIL,
		MESSAGE_STATUS, /* Refer to message.status */
	}
	
	public OptCode opt;
	
	public OptState state;
	
	public RequestKVMessage message;
	
	// Serialize a CommMessage into a byte array that represents a JSON string.
	public static byte[] serialize(CommMessage msg) {
		Gson gson = new GsonBuilder().serializeNulls().create();
		return gson.toJson(msg).getBytes();
	}
	
	// Deserialize a CommMessage from given byte array data. Throws JsonSyntaxException
	// if the JSON string embedded in data is not of the right format.
	public static CommMessage deserialize(byte[] data) throws JsonSyntaxException{
		Gson gson = new GsonBuilder().serializeNulls().create();
		return gson.fromJson(new String(data), CommMessage.class);
	}
}
