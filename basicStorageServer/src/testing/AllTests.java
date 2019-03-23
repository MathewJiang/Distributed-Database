package testing;

import java.io.IOException;

import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;

import org.apache.log4j.Level;

import app_kvServer.KVServer;


public class AllTests {

//	static {
//		try {
//			new LogSetup("logs/testing/test.log", Level.ERROR);
//			KVServer srv = new KVServer(50000, 10, "LRU");
//			srv.start();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
	
	
	public static Test suite() {
		// Note: need to clean up processes after running the tests
		
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		// M1 test cases (deprecated after infrastructure change)
//		clientSuite.addTestSuite(ConnectionTest.class);
//		clientSuite.addTestSuite(InteractionTest.class);
//		clientSuite.addTestSuite(BadKeyTest.class);
//		clientSuite.addTestSuite(AdditionalTest.class);
//		clientSuite.addTestSuite(ConcurrencyTest.class);		//should not be run here
																//has its own setup procedure
		
		// M2 tests
//		clientSuite.addTestSuite(AckTest.class);	// pass for M3
//		clientSuite.addTestSuite(LockTest.class);	// pass for M3
		clientSuite.addTestSuite(ECSTest.class);
//		clientSuite.addTestSuite(MD5Test.class);	// pass for M3
//		clientSuite.addTestSuite(MigrationTest.class);
//		clientSuite.addTestSuite(ConsistentHashTest.class);	// pass for M3
//		clientSuite.addTestSuite(M1Test.class);		// pass for M3
		
		// M3 Test
//		clientSuite.addTestSuite(ReplicationTest.class);
		return clientSuite;
	}

}
