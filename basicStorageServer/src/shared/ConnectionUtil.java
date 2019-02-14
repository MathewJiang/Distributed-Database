package shared;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import shared.messages.CommMessage;
import app_kvServer.KVServer;

import com.google.gson.JsonSyntaxException;

public class ConnectionUtil {
	private Logger logger = Logger.getRootLogger();
	
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	private static final byte terminateByte = '\n';
	
	public void sendCommMessage(OutputStream output, CommMessage message) throws IOException {
		byte[] msgBytes = CommMessage.serialize(message);
		byte[] sendBytes = addTerminateByte(msgBytes);
		output.write(sendBytes, 0, sendBytes.length);
		output.flush();
		logger.info("Send message:\t '" + message.toString() + "'");
	}
	
	private byte[] addTerminateByte(byte[] msgBytes) {
		byte[] result = new byte[msgBytes.length + 1];
		for (int i = 0; i < msgBytes.length; i++) result[i] = msgBytes[i];
		result[result.length - 1] = terminateByte;
		return result;
	}
	
	public CommMessage receiveCommMessage(InputStream input) throws IOException {
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) 0;
		while (KVServer.serverOn) {
			if (input.available() > 0) {
				read = (byte) input.read();
				break;
			}
		}
		
		if (!KVServer.serverOn) {
			return null;					//server is down
											//this also makes the JUnit: InteractiveTest fail
		}
		
//		byte read = (byte) input.read();
		boolean reading = true;

		while (read != 13 && reading) {/* carriage return */
			/* if buffer filled, copy to message array */
			if (index == BUFFER_SIZE) {
				if (msgBytes == null) {
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			}

			/* only read valid characters, i.e. letters and numbers */
			if ((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}

			/* stop reading is DROP_SIZE is reached */
			if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}

			// If the current byte read is the termination byte,
			// stop reading.
			if (read == terminateByte) {
				reading = false;
				continue;
			}
			
			// FIXME: 
			// deal with cases where server shut down when client is sending a message
			// in this case, we should still try to process the request
			// only quit if only partial message has been sent
			if (input.available() == 0) {
				return null;
			} else {
				if (!KVServer.serverOn) {
					return null;
				} else {
					read = (byte) input.read();
				}
			}
//			read = (byte) input.read();
			
//			while (KVServer.serverOn) {
//				if (input.available() > 0) {
//					read = (byte) input.read();
//					break;
//				}
//			}
//			
//			if (!KVServer.serverOn) {
//				return null;
//			}
			
		}

		if (msgBytes == null) {
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}

		msgBytes = tmp;

		/* build final String */
		CommMessage msg = CommMessage.deserialize(msgBytes);
		if (msg == null) {
			throw new JsonSyntaxException("[ConnectionUtil]/receiveMsg(): gson deserilization failed");
		}

		logger.info("Receive message:\t '" + msg.toString() + "'");
		return msg;
	}
}
