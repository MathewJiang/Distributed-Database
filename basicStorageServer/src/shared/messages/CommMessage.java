package shared.messages;

import shared.InfraMetadata;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

public class CommMessage implements KVMessage {

	// Status and data of this CommMessage.
	private StatusType status;
	private String key;
	private String value;
	private InfraMetadata infraMetadata;
	private KVAdminMessage adminMessage;

	public CommMessage() {
		this.status = null;
		this.key = null;
		this.value = null;
	}

	public CommMessage(StatusType status, String key, String value) {
		this.status = status;
		this.key = key;
		this.value = value;
	}

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

	public KVAdminMessage getAdminMessage() {
		return this.adminMessage;
	}

	public void setAdminMessage(KVAdminMessage adminMessage) {
		this.adminMessage = adminMessage;
	}

	// Serialize a CommMessage into a byte array that represents a JSON string.
	public static byte[] serialize(CommMessage msg) {
		Gson gson = new GsonBuilder().serializeNulls().create();
		return gson.toJson(msg).getBytes();
	}

	// Deserialize a CommMessage from given byte array data. Throws Exception
	// if the JSON string embedded in data is not of the correct format.
	public static CommMessage deserialize(byte[] data)
			throws JsonSyntaxException {
		Gson gson = new GsonBuilder().serializeNulls().create();
		return gson.fromJson(new String(data), CommMessage.class);
	}

	@Override
	public String toString() {
		StringBuilder id = new StringBuilder();
		id.append("{ CommMessage Object: ");
		id.append("{ Status: ");
		id.append(this.status);
		id.append(" }");

		id.append("{ key: ");
		id.append(this.key);
		id.append(" }");

		id.append("{ value: ");
		id.append(this.value);
		id.append(" }");
		
		if (adminMessage != null) {
			id.append(" | {AdminMessage Object: ");
			id.append(adminMessage.getKVAdminMessageType().toString());
			id.append("} ");
		}
		
		id.append("}");

		return id.toString();
	}

	public InfraMetadata getInfraMetadata() {
		return infraMetadata;
	}

	public void setInfraMetadata(InfraMetadata infraMetadata) {
		this.infraMetadata = infraMetadata;
	}
}
