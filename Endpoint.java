/* 
 * Endpoint
 * Holds tuple of (host, port, key).
 */
public class Endpoint {
	public String host;
	public int port;
	public long key;
	
	/* Endpoint constructor. */
	public Endpoint(String host, int port) {
		this.host = host;
		this.port = port;
		this.key = Utils.hash(host + port);
	}

	/* Endpoint constructor. */
	public Endpoint(String host, int port, long key) {
		this.host = host;
		this.port = port;
		this.key = key;
	}
	
	@Override
	/* Return whether two endpoints are equal. */
	public boolean equals(Object obj) {
		Endpoint e = (Endpoint) obj;
		return e.host.equals(this.host) && e.port == this.port && e.key == this.key;
	}
	
	@Override
	/* Return endpoint in readable format. */
	public String toString() {
		return "(key: " + key + " | host: " + host + " | port: " + port + ")";
	}

	/* Serialize endpoint. */
	public static String serialize(Endpoint ep) {
		if (ep == null)
			return "null";
		return String.format("%s|%d|%d", ep.host, ep.port, ep.key);
	}

	/* Deserialize endpoint. */
	public static Endpoint deserialize(String s) {
		try {
			String[] info = s.split("\\|");
			if (info.length != 3) {
				return null;
			}

			String host = info[0];
			int port = Integer.parseInt(info[1]);
			long key = Long.parseLong(info[2]);
			return new Endpoint(host, port, key);
		} catch (Exception e) {
			return null;
		}
	}
}
