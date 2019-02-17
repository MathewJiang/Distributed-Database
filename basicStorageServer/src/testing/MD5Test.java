/*************************************************
 * MD5 Tests
 * 
 * Test for MD5 API's
 *************************************************/

package testing;

import static org.junit.Assert.*;

import java.math.BigInteger;

import org.junit.Before;
import org.junit.Test;

import shared.MD5;

public class MD5Test {

	@Test
	public void testString() {
		String serverName = "127.0.0.1:50001";
		String serverMD5String = MD5.getMD5String(serverName);
		
		assertTrue(serverMD5String.equals("dcee0277eb13b76434e8dcd31a387709"));
	}
	
	@Test
	public void testBigInteger() {
		String serverName = "127.0.0.1:50001";
		BigInteger serverMD5BigInteger = MD5.getMD5(serverName);
		
		assertTrue(serverMD5BigInteger.compareTo(new BigInteger("293665975790736140455366914418740328201")) == 0);
	}

}
