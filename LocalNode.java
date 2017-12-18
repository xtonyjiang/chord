import java.util.Map;

public class LocalNode extends AbstractNode {
	private Map<Integer, LocalNode> nodeMap;
	
	public LocalNode(Map<Integer, LocalNode> map, int port) {
		super("localhost", port);
		this.nodeMap = map;
	}
	
	@Override
	public void create() {
		super.create();
		nodeMap.put(myEp.port, this);
	}
	
	@Override
	public void join(Endpoint ep) {
		super.join(ep);
		nodeMap.put(myEp.port, this);
	}
	
	@Override
	public void leave() {
		super.leave();
		this.nodeMap.remove(myEp.port);
	}
	
	@Override
	Endpoint getSuccessor(Endpoint ep) {
		return nodeMap.get(ep.port).finger.get(0);
	}

	@Override
	Endpoint getSuccessor(Endpoint ep, long id) {
		return nodeMap.get(ep.port).findSuccessor(id);
	}

	@Override
	Endpoint getPredecessor(Endpoint ep) {
		return nodeMap.get(ep.port).predecessor;
	}

	@Override
	void giveItems(Endpoint ep, Map<Long, Long> items) {
		for (Map.Entry<Long, Long> e : items.entrySet()) {
			giveItem(ep, e.getKey(), e.getValue());
		}
	}
	
	@Override
	void giveItem(Endpoint ep, long key, long value) {
		nodeMap.get(ep.port).myItems.put(key, value);
	}
	
	@Override
	Long getItem(Endpoint ep, long key) {
		return nodeMap.get(ep.port).myItems.get(key);
	}

	@Override
	void notify(Endpoint ep) {
		nodeMap.get(ep.port).processNotification(myEp);
	}

	@Override
	Endpoint closestPrecedingFinger(Endpoint ep, long id) {
		for (int i = M - 1; i >= 0; --i) {
			if (nodeMap.get(ep.port).finger.get(i) == null) {
				continue;
			}
			if (inBetween(ep.key, id, nodeMap.get(ep.port).finger.get(i).key)) {
				return nodeMap.get(ep.port).finger.get(i);
			}
		}
		return ep;
	}

	@Override
	boolean isReachable(Endpoint ep) {
		return this.nodeMap.containsKey(ep.port);
	}
}
