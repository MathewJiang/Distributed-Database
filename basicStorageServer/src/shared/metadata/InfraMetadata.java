package shared.metadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Class that holds basic infrastructure metadata of the cluster. Can be parsed
 * from a properties file of the following format:
 * 
 * cluster.properties 0: ecs:host:port 1: server1:host:port 2: server2:host:port
 * ...
 */
public class InfraMetadata {
	public static class ServiceLocation {
		public String serviceName;
		public String host;
		public Integer port;

		public ServiceLocation(String name, String host, Integer port) {
			this.serviceName = name;
			this.host = host;
			this.port = port;
		}

		public String toString() {
			return serviceName + ":" + host + ":" + port;
		}
		
		public boolean equals(ServiceLocation sl) {
			return ((serviceName.equals(sl.serviceName))
					&& (host.equals(sl.host))
					&& (port.equals(sl.port)));
		}
	}
}
