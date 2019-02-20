package shared.metadata;

public class ServiceLocation {
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
