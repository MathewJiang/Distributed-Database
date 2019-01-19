package client;

import java.io.BufferedReader;
import java.io.IOException;

import app_kvClient.Client;
import app_kvClient.ClientSocketListener;
import shared.messages.CommMessage;
import shared.messages.KVMessage;
import shared.messages.CommMessage.OptCode;
import shared.messages.KVMessage.StatusType;

public class KVStore implements KVCommInterface, ClientSocketListener {
	
	private BufferedReader stdin;
	private Client client = null;
	private boolean stop = false;
	
	private String serverAddress;
	private int serverPort;
	
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		// TODO Auto-generated method stub
		this.serverAddress = address;
		this.serverPort = port;
	}

	@Override
	public void connect() throws Exception {
		// TODO Auto-generated method stub
		client = new Client(this.serverAddress, this.serverPort);
		client.addListener(this);
		client.start();
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
		if(client != null) {
			client.closeConnection();
			client = null;
		}
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		
		CommMessage cm = new CommMessage(StatusType.PUT, OptCode.PUT, key.toString(), value.toString());
		try {
			client.sendCommMessage(cm);
			CommMessage response = client.receiveCommMessage();
			
			return response;
		} catch(IOException e) {
			//TODO: set the status code
			System.out.println("[KVStore]PUT: Error! Command failed");
			disconnect();
		}
		return null;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void handleNewCommMessage(CommMessage cm) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void handleStatus(SocketStatus status) {
		// TODO Auto-generated method stub
		
	}
}
