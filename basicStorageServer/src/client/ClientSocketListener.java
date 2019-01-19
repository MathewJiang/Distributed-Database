package client;

import shared.messages.CommMessage;

public interface ClientSocketListener {

	public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};
	
	public void handleNewCommMessage(CommMessage cm);
	
	public void handleStatus(SocketStatus status);
}