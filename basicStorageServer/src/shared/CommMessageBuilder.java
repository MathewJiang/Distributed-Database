package shared;

import shared.messages.CommMessage;
import shared.messages.KVMessage;

public class CommMessageBuilder {
	private CommMessage msg = new CommMessage();
	
	public CommMessageBuilder setOpt(CommMessage.OptCode opt) {
		msg.opt = opt;
		return this;
	}
	
	public CommMessageBuilder setStatus(KVMessage.StatusType status) {
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
	
	public CommMessage build() {
		return msg;
	}
}
