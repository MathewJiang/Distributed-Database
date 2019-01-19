package app_kvClient;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.FileReader;

import shared.messages.KVMessage;

public class Disk {
	static String path = "";
	static String db_dir = "";
	public static void echo(String line) {
		System.out.println(line);
	}
	
	public static boolean if_init() {
		if(db_dir=="") {
			return false;
		}
		return true;
	}
	
	public static void test_and_set_db() {
		// mkdir at local dir if not being created
		db_dir = path + "/kvdb";
		File db = new File(db_dir);
		if(db.exists()) {
			echo("db exists");
		} else {
			echo("db does not exist, making a new db");
			if(db.mkdir()) {
				echo("Mkdir sucess");
			} else {
				echo("Mkdir failed");
			}
		}
		// this point, we have a db dir initialized
	}
	
	public static String getKV(String key) throws Exception {
		
		String src = db_dir+"/"+key;
		File search = new File(src);
		if(search.exists()) {
			// we have this key
			
		} else {
			// we don't have this key
			echo("Trying to search");
			throw new Exception();
		}
		
		FileReader fr = new FileReader(src);
		BufferedReader fh = new BufferedReader(fr);
		
		String result = "";
		String line;
		while((line = fh.readLine()) != null){
			result += line;
		}
		return result;
	}
	
	public static boolean inStorage(String key){
		
		String src = db_dir+"/"+key;
		File search = new File(src);
		if(search.exists()) {
			return true;
		} else {
			return false;
		}
	}
	
	public static KVMessage.StatusType putKV(String key, String value){
		echo("Disk putKV("+key+","+value+')');
		String dest = db_dir+"/"+key;
		File search = new File(dest);
		
		if(value.equals("null")) {
			File delete_file = new File(dest);
			if(!delete_file.delete()) {
				return KVMessage.StatusType.DELETE_ERROR;
			}
			return KVMessage.StatusType.DELETE_SUCCESS;
		}
		
		boolean is_update = false;
		if(search.exists()) {
			// we have this key, put update the value
			is_update = true;
			
		} else {
			// we don't have this key, put add the file
			try {
				search.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		try {
			PrintWriter key_file = new PrintWriter(dest);
			key_file.println(value);
			key_file.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(is_update) {
			return KVMessage.StatusType.PUT_UPDATE;
		}
		return KVMessage.StatusType.PUT_SUCCESS;
	}
	
	public static int key_count() {
		db_dir = path + "/kvdb";
		File db = new File(db_dir);
		String[] entries = db.list();
		return entries.length;
	}
	
	public static void clearStorage(){
		db_dir = path + "/kvdb";
		File db = new File(db_dir);
		String[] entries = db.list();
		echo("Removing files under db_dir " + db_dir);
		for(int i = 0; i < entries.length; i++){
		    File curr = new File(db_dir + '/' + entries[i]);
		    /*if(curr.isFile()) {
		    	echo(curr.getAbsolutePath() + " is file");
		    } else {
		    	echo(curr.getAbsolutePath() + " is not file");
		    }*/
		    curr.delete();
		}
		//echo("Removed files under db_dir");
	}

	public static void init() {
		path = System.getProperty("user.dir");
		String pwd_debug_info = "Working directory is " + path;
		echo(pwd_debug_info);
		test_and_set_db();
		
		
	}

}