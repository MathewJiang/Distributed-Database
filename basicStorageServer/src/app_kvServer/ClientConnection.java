package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.*;

import app_kvClient.Disk;

import shared.messages.CommMessage;
import shared.messages.KVMessage.StatusType;


/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();
	
	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	
	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			
			while(isOpen) {
				try {
					CommMessage latestMsg = receiveCommMessage();
					CommMessage responseMsg = new CommMessage();
					
					StatusType op = latestMsg.getStatus();
					String key = latestMsg.getKey();
					String value = latestMsg.getValue();
					
					if (op.equals(StatusType.PUT)) {
						if (!value.equals("null")) {
							//put option
							try {
								handlePUT(key, value);
								responseMsg.setStatus(StatusType.PUT_SUCCESS);
							} catch (IOException e) {
								responseMsg.setStatus(StatusType.PUT_ERROR);
							}
						} else {
							//delete option
							try {
								handlePUT(key, value);
								responseMsg.setStatus(StatusType.PUT_SUCCESS);
							} catch (IOException e) {
								responseMsg.setStatus(StatusType.PUT_ERROR);
							}
						}
					} else if (op.equals(StatusType.GET)) {
						try {
							value = handleGET(key);
							
							responseMsg.setKey(key);
							responseMsg.setValue(value);
							responseMsg.setStatus(StatusType.GET_SUCCESS);
						} catch (Exception e) {
							value = null;
							responseMsg.setStatus(StatusType.GET_ERROR);
						}
					}
					sendCommMessage(responseMsg);
					
				/* connection either terminated by the client or lost due to 
				 * network problems*/	
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				}				
			}
			
		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);
			
		} finally {
			
			try {
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}
	
	
	/**
	 * Method sends a CommMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public void sendCommMessage(CommMessage msg) throws IOException {
		byte[] msgBytes = CommMessage.serialize(msg);
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("Comm SEND \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg.toString() +"'");
    }
	
	
	private CommMessage receiveCommMessage() throws IOException {
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;
		
//		logger.info("First Char: " + read);
//		Check if stream is closed (read returns -1)
//		if (read == -1){
//			TextMessage msg = new TextMessage("");
//			return msg;
//		}

		while(/*read != 13  && */ read != 10 && read !=-1 && reading) {/* CR, LF, error */
			/* if buffer filled, copy to msg array */
			if(index == BUFFER_SIZE) {
				if(msgBytes == null){
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			} 
			
			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;
			
			/* stop reading is DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}
			
			if (read == 125) {
				
				//FIXME: hardcoded the ending character...
				//if reaches the end of gson
				reading = false;
			} else {
				/* read next char from stream */
				read = (byte) input.read();
			}
		}
		
		if(msgBytes == null){
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}
		
		msgBytes = tmp;
		
		
		/* build final String */
		CommMessage cm = CommMessage.deserialize(msgBytes);
		if (cm == null) {
			throw new IOException();
		}
		
		logger.info("Comm RECEIVE \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ cm.toString().trim() + "'");
		
		
		return cm;
    }
	

	/**********************************
	 * Helper methods
	 **********************************/
	
	/*
	 * handlePUT
	 * store the <key, value> pairs in persistent disk
	 */
	private void handlePUT(String key, String value) throws IOException {
		if (!Disk.if_init()) {
			logger.warn("[ClientConnection]handlePUT: DB not initalized during Server startup");
			Disk.init();		//FIXME: should raise a warning/error
		}
		
		Disk.putKV(key, value);
	}
	
	private void handleDELETE(String key, String value) throws IOException {
		if (!Disk.if_init()) {
			logger.warn("[ClientConnection]handlePUT: DB not initalized during Server startup");
			Disk.init();		//FIXME: should raise a warning/error
		}
		
		Disk.putKV(key, value);
	}
	
	private String handleGET(String key) throws Exception {
		if (!Disk.if_init()) {
			logger.warn("[ClientConnection]handleGET: DB not initalized during Server startup");
		}
		
		return Disk.getKV(key);
		
	}
	
}
