package shared;

import java.io.IOException;

import app_kvClient.IKVClient;
import app_kvClient.KVClient;
import app_kvServer.IKVServer;
import app_kvServer.KVServer;

public final class ObjectFactory {
	/*
	 * Creates a KVClient object for auto-testing purposes
	 */
    public static IKVClient createKVClientObject() {
    	try {
			return new KVClient();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    	return null;
    }
    
    /*
     * Creates a KVServer object for auto-testing purposes
     */
	public static IKVServer createKVServerObject(int port, int cacheSize, String strategy) {
		return new KVServer(port, cacheSize, strategy);
	}
}