package testing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

import org.junit.Test;

import app_kvClient.Disk;

import junit.framework.TestCase;

public class AdditionalTest extends TestCase {
	
	// TODO add your test cases, at least 3
	static String path = "";
	
	@Test
	public void testStub() {
		
		path = System.getProperty("user.dir");
		
		File test_log = new File(path+"perf.log");
		if(test_log.exists()) {
			test_log.delete();
			
		}
		try {
			test_log.createNewFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		int i = 100;
		while(i < 10000) {
			testDiskPerf(i);
			i+=100;
		}
	}
	
	public long testDiskPerf(int num_files) {
    	Disk.init(); // must init first
    	Disk.clearStorage();
		
    	final long startTime = System.currentTimeMillis();
    	Disk.putKV("a", "1209");
    	for(int i = 0; i < num_files; i++) {
    		Disk.putKV(((Integer)i).toString(), ((Integer)i).toString());
    	}
    	
    	try {
			String read = Disk.getKV("a");
			Disk.echo(read);
			read = Disk.getKV("8475");
			Disk.echo(read);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	final long endTime = System.currentTimeMillis();
    	String perfTime = "num_files = " + num_files + " Total used " + (endTime - startTime) + "\n";
    	append(path+"perf.log", perfTime);
    	return (endTime - startTime);
	}
	
	public static void append(String file_path, String content) {
		OutputStream os;
		try {
			os = new FileOutputStream(new File(file_path), true);
			os.write(content.getBytes(), 0, content.length());
			os.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
