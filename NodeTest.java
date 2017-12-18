import java.util.*;

public class NodeTest {
	public static void assertEquals(long o1, long o2) throws Exception {
		if (o1 != o2) {
			throw new Exception("Assertion failed: " + o1 + " is not equal to " + o2 + ".");
		}
	}
	
	public static void assertFalse(boolean b) throws Exception {
		if (b) {
			throw new Exception("Assertion failed: Expected false");
		}
	}
	
	public static void assertNull(Object o) throws Exception {
		if (o != null) {
			throw new Exception("Assertion failed: " + o + " is not null.");
		}
	}
	
	public void twoNodeTest() throws Exception {
		Map<Integer, LocalNode> m = new HashMap<>();
		int port1 = 3;
		int port2 = 6;
		LocalNode n1 = new LocalNode(m, port1);
		LocalNode n2 = new LocalNode(m, port2);
		
		n1.create();
		n2.join(n1.myEp);
		assertEquals(port1, n2.getSuccessor().port);
		assertEquals(port1, n1.getSuccessor().port);  // hasn't stabilized yet
		
		// test stabilization
		n2.stabilize();
		assertEquals(port1, n2.getSuccessor().port);
		assertEquals(port2, n1.predecessor.port);
		n1.stabilize();
		assertEquals(port2, n1.getSuccessor().port);
		assertEquals(port1, n2.predecessor.port);
		
		// test add item and retrieve item
		long key1 = 1;
		long key2 = 2;
		n1.put(key1, 10);  // key1 gets stored in n1
		assertEquals(10, (long) n1.get(key1));
		assertEquals(10, (long) n2.get(key1));
		
		n2.put(key1, 20);
		assertEquals(20, (long) n1.get(key1));
		assertEquals(20, (long) n2.get(key1));
		
		n1.put(key2, 10);  // key2 gets stored in n2
		assertEquals(10, (long) n1.get(key2));
		assertEquals(10, (long) n2.get(key2));
		
		n2.put(key2, 20);
		assertEquals(20, (long) n1.get(key2));
		assertEquals(20, (long) n2.get(key2));
		
		// test get null item
		assertNull(n1.get(0));
		assertNull(n2.get(0));
		
		assertEquals(n1.myItems.size(), 1);
		assertEquals(n2.myItems.size(), 1);
		
		// test leaving
		n1.leave();
		assertEquals(n2.myItems.size(), 2);
		assertEquals(20, (long) n2.get(key1));
		assertEquals(20, (long) n2.get(key2));
		assertFalse(n2.checkPredecessor());
		n2.stabilize();
		assertEquals(port2, n2.getSuccessor().port);
		
		// test rejoining
		n1.join(n2.myEp);
		n1.stabilize();
		n2.stabilize();
		assertEquals(port2, n1.getSuccessor().port);
		assertEquals(port1, n2.predecessor.port);
		assertEquals(port2, n1.getSuccessor().port);
		assertEquals(port1, n2.predecessor.port);
		assertEquals(n1.myItems.size(), 1);
		assertEquals(n2.myItems.size(), 1);
	}

	public void manyNodesTest() throws Exception {
		final int numNodes = 10;
		Map<Integer, LocalNode> m = new HashMap<>();
		LocalNode[] nodes = new LocalNode[numNodes];
		for (int i = 0; i < numNodes; ++i) {
			nodes[i] = new LocalNode(m, i);
			if (i == 0) {
				nodes[i].create();
			} else {
				nodes[i].join(nodes[0].myEp);
			}
		}

		for (int i = 0; i < numNodes; ++i) {
			for (int j = 0; j < numNodes; ++j) {
				nodes[j].stabilize();
				for (int k = 0; k < AbstractNode.M; ++k) {
					nodes[j].fixFingers();
				}
			}
		}

		for (int i = 0; i < 100; ++i) {
			nodes[numNodes - 1 - (i % numNodes)].put(i, i);
			assertEquals(i, (long) nodes[i % numNodes].get(i));
		}
	}
	
	public static void main(String args[]) throws Exception {
		NodeTest test = new NodeTest();
		test.twoNodeTest();
		test.manyNodesTest();
		System.out.println("Tests succeeded!");
	}
}
