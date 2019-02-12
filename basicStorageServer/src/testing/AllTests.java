package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
			KVServer srv = new KVServer(50000, 10, "LRU");
			srv.start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class);
		clientSuite.addTestSuite(BadKeyTest.class);
		clientSuite.addTestSuite(AdditionalTest.class);
		clientSuite.addTestSuite(ConcurrencyTest.class);		//should not be run here
																//has its own setup procedure
		
		return clientSuite;
	}

}
