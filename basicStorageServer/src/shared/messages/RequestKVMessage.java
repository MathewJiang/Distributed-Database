package shared.messages;

public class RequestKVMessage implements KVMessage {
	
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
	
}
