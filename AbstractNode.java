import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.Map;

/* 
 * AbstractNode
 * Abstract class for a node.
 */
public abstract class AbstractNode {
	public static final int M = 31;
	public static final long MOD = (1L << M);

	protected Map<Integer, Endpoint> finger;
	protected Endpoint predecessor;
	protected Endpoint myEp;
	protected Map<Long, Long> myItems;
	private int nextToFix;
	
	/* AbstractNode constructor. */
	public AbstractNode(String host, int port) {
		finger = new ConcurrentHashMap<Integer, Endpoint>(M);
		myEp = new Endpoint(host, port);
		myItems = new ConcurrentHashMap<Long, Long>();
		nextToFix = 0;
	}

	// ==============================
  // Core Functions
  // ==============================

	/* Create new chord. */
	public void create() {
		finger.put(0, myEp);
		Debug.DEBUG("CREATING: " + myEp);
	}
	
	/* Join existing chord. */
	public void join(Endpoint ep) {
		finger.put(0, getSuccessor(ep, myEp.key));
		Debug.DEBUG("JOINING: " + myEp);
	}

	/* Leave chord. */
	public void leave() {
		giveItems(getSuccessor(), myItems);
		myItems.clear();
	}
	
	/* Get value associated with key. */
	public Long get(long key) {
		if (myItems.containsKey(key)) {
			return myItems.get(key);
		}
		return getItem(findSuccessor(Utils.hash(key)), key);
	}
	
	/* Put (key, value) pair. */
	public void put(long key, long value) {
		long h = Utils.hash(key);
		Endpoint ep = findSuccessor(h);
		Debug.DEBUG("STORING: " + key + " (" + h + ") in " + ep);
		
		giveItem(ep, key, value);
	}

	// ==============================
  // Additional Functions
  // ==============================

	/* Get successor and update first finger. */
	public Endpoint getSuccessor() {
		for (int i = 0; i < M; ++i) {
			Endpoint cur = finger.get(i);
			if (cur != null && isReachable(cur)) {
				finger.put(0, cur);
				return cur;
			}
		}
		finger.put(0, myEp);
		return myEp;
	}
	
	/* Find successor from id. */
	public Endpoint findSuccessor(long id) {
		return getSuccessor(findPredecessor(id));
	}
	
	/* Find predecessor from id. */
	public Endpoint findPredecessor(long id) {
		Endpoint cur = myEp;
		Endpoint curSuccessor = getSuccessor(cur);
		if (cur.key == curSuccessor.key) {
			return cur;
		}
		while (!inBetween(cur.key, (curSuccessor.key + 1) % MOD, id)) {
			cur = closestPrecedingFinger(cur, id);
			curSuccessor = getSuccessor(cur);
			if (cur.key == myEp.key || cur.key == curSuccessor.key) {
				return cur;
			}
		}
		return cur;
	}

	/* Receive notification from ep (ep thinks it is this node's predecssor). */
	public void processNotification(Endpoint ep) {
		// Debug.DEBUG("" + ep + " NOTIFYING " + myEp);
		if (predecessor == null || inBetween(predecessor.key, myEp.key, ep.key)) {
			predecessor = ep;

			// give keys to new predecessor
			if (!this.myEp.equals(predecessor)) {
				Map<Long, Long> toGive = new HashMap<>();
				for (Map.Entry<Long, Long> e : myItems.entrySet()) {
					if (!inBetween(ep.key, (myEp.key + 1) % MOD, Utils.hash(e.getKey()))) {
						toGive.put(e.getKey(), e.getValue());
					}
				}
				giveItems(ep, toGive);
				for (Long key : toGive.keySet()) {
					myItems.remove(key);
				}
			}
		}
	}

	// ==============================
  // Periodic Functions
  // ==============================

	/* Stabilize to learn about newly joined nodes. */
	public void stabilize() {
		Endpoint successor = getSuccessor();
		Endpoint x = getPredecessor(successor);
		if (x == null || x.equals(successor) || inBetween(x.key, successor.key, myEp.key)) {
			notify(successor);
		}
		else if (inBetween(myEp.key, successor.key, x.key)) {
			finger.put(0, x);
			notify(x);
		}
	}
	
	/* Update finger table. */
	public void fixFingers() {
		finger.put(nextToFix, findSuccessor((myEp.key + (1 << nextToFix)) % MOD));
		nextToFix = (nextToFix + 1) % M;
	}
	
	/* Return whether predecssor is reachable. Set predecessor to null if not. */
	public boolean checkPredecessor() {
		if (predecessor == null) {
			return false;
		}
		if (!isReachable(predecessor)) {
			predecessor = null;
			return false;
		}
		return true;
	}

	// ==============================
  // Utility Functions
  // ==============================

	/* Return whether id is in the interval (left, right) on the circle. */
	protected boolean inBetween(long left, long right, long id) {
		if (left < right) {
			return id > left && id < right;
		} else if (left > right) {
			return id > left || id < right;
		} else {
			return true;
		}
	}

	// ==============================
  // Abstract Functions
  // ==============================
	
	/* Find closest preceding finger from id. */
	abstract Endpoint closestPrecedingFinger(Endpoint ep, long id);
	
	/* Get successor of ep. */
	abstract Endpoint getSuccessor(Endpoint ep);
	
	/* Get successor of ep from id. */
	abstract Endpoint getSuccessor(Endpoint ep, long id);
	
	/* Get predecessor of ep. */
	abstract Endpoint getPredecessor(Endpoint ep);
	
	/* Get value associated with key from ep. */
	abstract Long getItem(Endpoint ep, long key);
	
	/* Give (key, value) pair to ep. */
	abstract void giveItem(Endpoint ep, long key, long value);
	
	/* Give items to ep. */
	abstract void giveItems(Endpoint ep, Map<Long, Long> items);
	
	/* Notify ep that this node may be its predecessor. */
	abstract void notify(Endpoint ep);
	
	/* Return whether ep is reachable. */
	abstract boolean isReachable(Endpoint ep);
}
