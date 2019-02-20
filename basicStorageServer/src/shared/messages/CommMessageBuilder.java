package shared.messages;

import shared.messages.KVMessage.StatusType;
import shared.metadata.InfraMetadata;

public class CommMessageBuilder {
	private CommMessage msg = new CommMessage();
	
	public CommMessageBuilder setStatus(StatusType status) {
		msg.setStatus(status);
		return this;
	}
	
	public CommMessageBuilder setKey(String key) {
		msg.setKey(key);
		return this;
	}
	
	public CommMessageBuilder setValue(String value) {
		msg.setValue(value);
		return this;
	}
	
	public CommMessageBuilder setInfraMetadata(InfraMetadata data) {
		msg.setInfraMetadata(data);
		return this;
	}
	
	public CommMessage build() {
		return msg;
	}
}
