package ecs;

public class ECSNode implements IECSNode {
	/**
	 * @param args
	 */
	private String NodeName = "";
	private String NodeHost = "";
	private int NodePort;
	private String[] NodeHashRange;
	public ECSNode(String node_name, String node_host, int node_port, String[] range) {
		NodeName = node_name;
		NodeHost = node_host;
		NodePort = node_port;
		NodeHashRange = range;
	}

	@Override
	public String getNodeName() {
		return NodeName;
	}

	@Override
	public String getNodeHost() {
		return NodeHost;
	}

	@Override
	public int getNodePort() {
		return NodePort;
	}

	@Override
	public String[] getNodeHashRange() {
		return NodeHashRange;
	}

}
