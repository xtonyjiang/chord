import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/* 
 * Node
 * Implementation for a node.
 */
public class Node extends AbstractNode implements Runnable {
	public static final String CREATE = "CREATE";
	public static final String JOIN = "JOIN";
	public static final String LEAVE = "LEAVE";
	public static final String GET = "GET";
	public static final String PUT = "PUT";

	public static final String PRE_FINGER = "PRE_FINGER";
	public static final String GET_SUCC = "GET_SUCC";
	public static final String GET_PRED = "GET_PRED";
	public static final String GET_ITEM = "GET_ITEM";
	public static final String GIVE_ITEM = "GIVE_ITEM";
	public static final String NOTIFY = "NOTIFY";
	public static final String IS_REACHABLE = "IS_REACHABLE";

	private static final String[] CORE_VALUES = new String[] {
			CREATE, JOIN, LEAVE, GET, PUT
	};
	private static final String[] ABSTRACT_VALUES = new String[] {
			PRE_FINGER, GET_SUCC, GET_PRED, GET_ITEM,
			GIVE_ITEM, NOTIFY, IS_REACHABLE
	};
	private static final Set<String> CORE_SET =
			new HashSet<String>(Arrays.asList(CORE_VALUES));
	private static final Set<String> ABSTRACT_SET =
			new HashSet<String>(Arrays.asList(ABSTRACT_VALUES));

	private static final int PERIODIC_DELAY = 200;

	private ScheduledExecutorService executor;
	private ScheduledFuture<?> periodicFuture;
	private boolean periodicScheduled;
	private ArrayList<Thread> threads;
	private ServerSocket ss;
	
	/* Node constructor. */
	public Node(String host, int port) throws IOException {
		super(host, port);
		executor = Executors.newScheduledThreadPool(1);
		periodicScheduled = false;
		threads = new ArrayList<Thread>();
		ss = new ServerSocket(port);
	}

	/* Verify input number of arguments. */
	private static boolean verifyNumArgs(String[] info) {
		return (info[0].equals(CREATE) && info.length != 1) ||
				(info[0].equals(JOIN) && info.length != 3) ||
				(info[0].equals(LEAVE) && info.length != 1) ||
				(info[0].equals(GET) && info.length != 2) ||
				(info[0].equals(PUT) && info.length != 3) ||
				(info[0].equals(PRE_FINGER) && info.length != 2) ||
				(info[0].equals(GET_SUCC) && info.length != 1 && info.length != 2) ||
				(info[0].equals(GET_PRED) && info.length != 1) ||
				(info[0].equals(GET_ITEM) && info.length != 2) ||
				(info[0].equals(GIVE_ITEM) && info.length != 3) ||
				(info[0].equals(NOTIFY) && info.length != 2) ||
				(info[0].equals(IS_REACHABLE) && info.length != 1);
	}

	/* Initialize socket from endpoint. */
	private static Socket getSocket(Endpoint ep) throws IOException {
		return new Socket(ep.host, ep.port);
	}

	/* Close socket under try-catch. */
	private static void closeSocket(Socket s) {
		try {
			s.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/* Runnable create. */
	private void rCreate(Socket s) {
		create();
		closeSocket(s);
		if (!periodicScheduled) {
			periodicScheduled = true;
			// execute periodic function
			periodicFuture = executor.scheduleAtFixedRate(() -> rPeriodic(),
					0, PERIODIC_DELAY, TimeUnit.MILLISECONDS);
		}
	}

	/* Runnable join. */
	private void rJoin(Socket s, Endpoint ep) {
		join(ep);
		closeSocket(s);
		if (!periodicScheduled) {
			periodicScheduled = true;
			// execute periodic function
			periodicFuture = executor.scheduleAtFixedRate(() -> rPeriodic(),
					0, PERIODIC_DELAY, TimeUnit.MILLISECONDS);
		}
	}

	/* Runnable leave. */
	private synchronized void rLeave(Socket s) {
		leave();
		closeSocket(s);
	}

	/* Runnable get. */
	private void rGet(Socket s, long key) {
		Long getValue = get(key);

		try {
			DataOutputStream sdos = Utils.getOutputStream(s);
			sdos.writeBytes(Objects.toString(getValue));
			sdos.flush();
			closeSocket(s);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/* Runnable put. */
	private void rPut(Socket s, long key, long value) {
		put(key, value);
		closeSocket(s);
	}

	/* Runnable periodic. */
	private synchronized void rPeriodic() {
		stabilize();
		fixFingers();
		checkPredecessor();
	}

	@Override
	/* Run method. */
	public void run() {
		while (true) {
			try {
				// accept socket and set up socket streams
				Socket s = ss.accept();
				BufferedReader sbr = Utils.getInputStream(s);
				DataOutputStream sdos = null;

				// read in single line of input
				String line = sbr.readLine();
				String[] info = line.split("\\s+");

				// verify number of arguments
				if (verifyNumArgs(info)) {
					System.err.println("ERROR: input invalid");
					s.close();
					continue;
				}

				Thread thread = null;

				Endpoint ep;
				String msg;
				String remotehost;
				int remoteport;
				long id;
				long key;
				Long getValue;
				long putValue;

				// set up socket output stream
				if (ABSTRACT_SET.contains(info[0])) {
					sdos = Utils.getOutputStream(s);
				}

				// respond to socket input
				switch (info[0]) {
					case CREATE:
						thread = new Thread(() -> rCreate(s));
						break;
					case JOIN:
						remotehost = info[1];
						remoteport = Integer.parseInt(info[2]);
						ep = new Endpoint(remotehost, remoteport);
						thread = new Thread(() -> rJoin(s, ep));
						break;
					case LEAVE:
						ss.close();
						thread = new Thread(() -> rLeave(s));
						break;
					case GET:
						key = Long.parseLong(info[1]);
						thread = new Thread(() -> rGet(s, key));
						break;
					case PUT:
						key = Long.parseLong(info[1]);
						putValue = Long.parseLong(info[2]);
						thread = new Thread(() -> rPut(s, key, putValue));
						break;
					case PRE_FINGER:
						id = Long.parseLong(info[1]);
						ep = closestPrecedingFinger(myEp, id);
						msg = Endpoint.serialize(ep);
						sdos.writeBytes(msg);
						sdos.flush();
						break;
					case GET_SUCC:
						if (info.length == 1) {
							ep = getSuccessor(myEp);
						} else {
							id = Long.parseLong(info[1]);
							ep = getSuccessor(myEp, id);
						}
						msg = Endpoint.serialize(ep);
						sdos.writeBytes(msg);
						sdos.flush();
						break;
					case GET_PRED:
						ep = getPredecessor(myEp);
						msg = Endpoint.serialize(ep);
						sdos.writeBytes(msg);
						sdos.flush();
						break;
					case GET_ITEM:
						key = Long.parseLong(info[1]);
						getValue = getItem(myEp, key);
						msg = Objects.toString(getValue);
						sdos.writeBytes(msg);
						sdos.flush();
						break;
					case GIVE_ITEM:
						key = Long.parseLong(info[1]);
						putValue = Long.parseLong(info[2]);
						giveItem(myEp, key, putValue);
						break;
					case NOTIFY:
						ep = Endpoint.deserialize(info[1]);
						processNotification(ep);
						break;
					case IS_REACHABLE:
						sdos.writeBytes(IS_REACHABLE);
						sdos.flush();
						break;
					default:
						System.err.println("ERROR: input invalid");
				}

				// execute separate thread
				if (CORE_SET.contains(info[0])) {
					threads.add(thread);
					thread.start();

					// exit while loop if leaving
					if (info[0].equals(LEAVE)) {
						break;
					}
				} else {
					s.close();
				}
			} catch (NumberFormatException e) {
				System.err.println("ERROR: input invalid");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// cancel scheduled future and executor
		if (periodicScheduled) {
			periodicFuture.cancel(true);
			executor.shutdown();
		}

		// join all threads
		for (Thread thread : threads) {
			try {
				thread.join();
			} catch (InterruptedException e) {}
		}
	}

	/* Send a request with header and args. */
	String request(Endpoint ep, String header, String args, boolean wait) {
		String response = null;

		try (Socket s = getSocket(ep);) {
			DataOutputStream sdos = Utils.getOutputStream(s);
			BufferedReader sbr = Utils.getInputStream(s);

			// send request
			String msg = String.format("%s%s\r\n", header, args);
			sdos.writeBytes(msg);
			sdos.flush();

			if (wait) {
				response = sbr.readLine();
			}
		} catch (Exception e) {}

		return response;
	}

	@Override
	/* closestPrecedingFinger implementation. */
	Endpoint closestPrecedingFinger(Endpoint ep, long id) {
		if (ep.equals(myEp)) {
			for (int i = M - 1; i >= 0; --i) {
				Endpoint cur = finger.get(i);
				if (cur == null) {
					continue;
				} else if (inBetween(myEp.key, id, cur.key)) {
					return cur;
				}
			}
			return myEp;
		}

		String args = String.format(" %d", id);
		String response = request(ep, PRE_FINGER, args, true);
		return Endpoint.deserialize(response);
	}

	@Override
	/* getSuccessor implementation. */
	Endpoint getSuccessor(Endpoint ep) {
		if (ep.equals(myEp)) {
			return finger.get(0);
		}

		String response = request(ep, GET_SUCC, "", true);
		return Endpoint.deserialize(response);
	}

	@Override
	/* getSuccessor implementation. */
	Endpoint getSuccessor(Endpoint ep, long id) {
		if (ep.equals(myEp)) {
			return findSuccessor(id);
		}

		String args = String.format(" %d", id);
		String response = request(ep, GET_SUCC, args, true);
		return Endpoint.deserialize(response);
	}

	@Override
	/* getPredecessor implementation. */
	Endpoint getPredecessor(Endpoint ep) {
		if (ep.equals(myEp)) {
			return predecessor;
		}

		String response = request(ep, GET_PRED, "", true);
		return Endpoint.deserialize(response);
	}

	@Override
	/* getItem implementation. */
	Long getItem(Endpoint ep, long key) {
		if (ep.equals(myEp)) {
			return myItems.get(key);
		}

		String args = String.format(" %d", key);
		String response = request(ep, GET_ITEM, args, true);

		try {
			return (response.equals("null")) ? null : Long.valueOf(response);
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override
	/* giveItem implementation. */
	void giveItem(Endpoint ep, long key, long value) {
		if (ep.equals(myEp)) {
			myItems.put(key, value);
			return;
		}

		String args = String.format(" %d %d", key, value);
		request(ep, GIVE_ITEM, args, false);
	}

	@Override
	/* giveItems implementation. */
	void giveItems(Endpoint ep, Map<Long, Long> items) {
		Long key;
		Long value;

		for (Map.Entry<Long, Long> e : items.entrySet()) {
			key = e.getKey();
			value = e.getValue();

			if (!ep.equals(myEp)) {
				System.out.println(ep);
				System.out.println(myEp);
				Debug.DEBUG("GIVING: " + "(" + key + " " + value + ") to " + ep);
			}

			giveItem(ep, key, value);
		}
	}

	@Override
	/* notify implementation. */
	void notify(Endpoint ep) {
		if (ep.equals(myEp)) {
			processNotification(myEp);
			return;
		}

		String args = String.format(" %s", Endpoint.serialize(myEp));
		request(ep, NOTIFY, args, false);
	}

	@Override
	/* isReachable implementation. */
	boolean isReachable(Endpoint ep) {
		if (ep.equals(myEp)) {
			return true;
		}

		String response = request(ep, IS_REACHABLE, "", true);
		return IS_REACHABLE.equals(response);
	}
}
