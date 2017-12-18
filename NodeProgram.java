import java.io.*;
import java.net.*;
import java.util.*;

/*
 * NodeProgram
 * Program for creating nodes and inputting commands.
 */
public class NodeProgram {
	private static final int NEW_NODE_DELAY = 100;
	private static final int SHUTDOWN_DELAY = 2000;

	private static Set<Integer> activePorts;
	private static ArrayList<Thread> threads;

	private static InetAddress localhostIA;

	/* Initialize variables. */
	private static void init() throws UnknownHostException {
		activePorts = new HashSet<Integer>();
		threads = new ArrayList<Thread>();

		localhostIA = InetAddress.getLocalHost();
	}

	/* Print usage information. */
	private static void usage() {
		System.out.println("Welcome to NodeProgram! Usage is provided below:\n" +
				"\tcreate <localport>\n" +
				"\tjoin <localport> <remotehost> <remoteport>\n" +
				"\tleave <localport>\n" +
				"\tget <localport> <key>\n" +
				"\tput <localport> <key> <value>\n" +
				"\tquit"
		);
	}

	/* Verify input number of arguments. */
	private static boolean verifyNumArgs(String[] info) {
		return (info[0].equals("create") && info.length != 2) ||
				(info[0].equals("join") && info.length != 4) ||
				(info[0].equals("leave") && info.length != 2) ||
				(info[0].equals("get") && info.length != 3) ||
				(info[0].equals("put") && info.length != 4);
	}

	/* Main method. */
	public static void main(String args[]) throws Exception {
		init();
		usage();

		String localhost = localhostIA.getHostAddress();

		// set up buffered reader for stdin
		InputStreamReader isr = new InputStreamReader(System.in);
		BufferedReader br = new BufferedReader(isr);

		// read in continuous input
		String line;
		while ((line = br.readLine()) != null && !line.equals("quit")) {
			if (line.equals("")) {
				continue;
			}

			// split line on whitespace
			String[] info = line.split("\\s+");

			// verify number of arguments
			if (info.length < 2 || verifyNumArgs(info)) {
				System.err.println("ERROR: input invalid");
				continue;
			}

			int localport = 0;
			try {
				localport = Integer.parseInt(info[1]);

				// create new node if "create" or "join" specified
				if (info[0].equals("create") || info[0].equals("join")) {
					if (!activePorts.contains(localport)) {
						activePorts.add(localport);
						Node n = new Node(localhost, localport);
						Thread thread = new Thread(n);
						threads.add(thread);
						thread.start();
					} else {
						System.err.println("ERROR: port number already in use");
						continue;
					}
				}
			} catch (Exception e) {
				System.err.println("ERROR: input invalid");
				continue;
			}

			// verify port is active
			if (!activePorts.contains(localport)) {
				System.err.println("ERROR: port number not found");
				continue;
			}

			try (Socket s = new Socket(localhost, localport);) {
				// open socket and set up socket streams
				DataOutputStream sdos = Utils.getOutputStream(s);
				BufferedReader sbr = null;
				if (info[0].equals("get")) {
					sbr = Utils.getInputStream(s);
				}

				String msg;
				String remotehost;
				int remoteport;
				long key;
				Long getValue;
				String getValueString;
				long putValue;

				// send message over socket
				switch (info[0]) {
					case "create":
						// tell node to create new chord
						msg = String.format("%s\r\n", Node.CREATE);
						sdos.writeBytes(msg);
						sdos.flush();
						break;
					case "join":
						// tell node to join existing chord
						remotehost = info[2];
						remoteport = Integer.parseInt(info[3]);
						msg = String.format("%s %s %d\r\n",
								Node.JOIN, remotehost, remoteport);
						sdos.writeBytes(msg);
						sdos.flush();
						break;
					case "leave":
						// tell node to leave chord
						activePorts.remove(localport);
						msg = String.format("%s\r\n", Node.LEAVE);
						sdos.writeBytes(msg);
						sdos.flush();
						break;
					case "get":
						// tell node to get value of key
						key = Long.parseLong(info[2]);
						msg = String.format("%s %d\r\n", Node.GET, key);
						sdos.writeBytes(msg);
						sdos.flush();

						// wait for corresponding value
						getValueString = sbr.readLine();
						if (getValueString.equals("null")) {
							System.out.println(getValueString);
						} else {
							getValue = Long.valueOf(getValueString);
							System.out.println(getValue.toString());
						}
						break;
					case "put":
						// tell node to put (key, value) pair
						key = Long.parseLong(info[2]);
						putValue = Long.parseLong(info[3]);
						msg = String.format("%s %d %d\r\n", Node.PUT, key, putValue);
						sdos.writeBytes(msg);
						sdos.flush();
						break;
					default:
						System.err.println("ERROR: input invalid");
				}

				if (info[0].equals("create") || info[0].equals("join")) {
					Thread.sleep(NEW_NODE_DELAY);
				}
			} catch (NumberFormatException e) {
				System.err.println("ERROR: input invalid");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		Thread.sleep(SHUTDOWN_DELAY);

		System.out.println("Shutting down...");

		// close all open nodes
		String msg = String.format("%s\r\n", Node.LEAVE);
		for (int localport : activePorts) {
			try (Socket s = new Socket(localhost, localport);) {
				// open socket and set up socket output stream
				DataOutputStream sdos = Utils.getOutputStream(s);

				// tell node to leave
				sdos.writeBytes(msg);
				sdos.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		activePorts.clear();

		// join all threads
		for (Thread thread : threads) {
			try {
				thread.join();
			} catch (InterruptedException e) {}
		}
	}
}
