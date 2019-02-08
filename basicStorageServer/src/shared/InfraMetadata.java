package shared;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

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
	}

	private ServiceLocation ecsLocation;
	private List<ServiceLocation> serverLocations = new ArrayList<ServiceLocation>();

	public static InfraMetadata fromConfigFile(String filePath)
			throws Exception {
		InfraMetadata result = new InfraMetadata();
		BufferedReader reader = new BufferedReader(new FileReader(filePath));

		// Read ECS config.
		String[] ecsConfig = reader.readLine().split(":");
		if (ecsConfig.length != 3) {
			reader.close();
			throw new Exception("Incorrect format for cluster config file");
		}
		result.ecsLocation = new ServiceLocation(ecsConfig[0], ecsConfig[1],
				Integer.valueOf(ecsConfig[2]));

		// Read server configs.
		String line;
		while ((line = reader.readLine()) != null) {
			String[] segments = line.split(":");
			if (segments.length != 3) {
				reader.close();
				throw new Exception("Incorrect format for cluster config file");
			}
			result.getServerLocations().add(
					new ServiceLocation(segments[0], segments[1], Integer
							.valueOf(segments[2])));
		}
		reader.close();
		return result;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(ecsLocation.toString() + "\n");
		for (ServiceLocation service : serverLocations)
			sb.append(service.toString() + "\n");
		sb.deleteCharAt(sb.length() - 1);
		return sb.toString();
	}

	public void toConfigFile(String destination) throws Exception {
		File f = new File(destination);
		if (f.exists())
			f.delete();
		FileOutputStream out = new FileOutputStream(destination);
		out.write(this.toString().getBytes());
		out.close();
	}

	public ServiceLocation getEcsLocation() {
		return ecsLocation;
	}

	public void setEcsLocation(ServiceLocation ecsLocation) {
		this.ecsLocation = ecsLocation;
	}

	public List<ServiceLocation> getServerLocations() {
		return serverLocations;
	}

	public void setEcsLocation(List<ServiceLocation> serverLocations) {
		this.serverLocations = serverLocations;
	}
}
