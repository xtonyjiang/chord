import java.io.*;
import java.net.*;
import java.nio.charset.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/* 
 * Utils
 * Utility functions for node implementation.
 */
public class Utils {
	public static final Charset CHARSET = StandardCharsets.UTF_8;

	/* SHA1 hash implementation. */
	private static byte[] sha1(String s) {
		byte[] ret = null;

		try {
			MessageDigest mDigest = MessageDigest.getInstance("SHA1");
			ret = mDigest.digest(s.getBytes());
		} catch (NoSuchAlgorithmException e) {
			System.err.println("EXCEPTION: SHA1 not found");
		}

		return ret;
	}

	/* Get hash of long based on first five bytes of SHA1 hash. */
	static long hash(long x) {
		return hash(sha1(new Long(x).toString()));
	}
	
	/* Get hash of string based on first five bytes of SHA1 hash. */
	static long hash(String s) {
		return hash(sha1(s));
	}
	
	/* Compute hash. */
	private static long hash(byte[] byteArray) {
		return Math.abs((long) byteArray[0] +
				((long) byteArray[1] << 8) + 
				((long) byteArray[2] << 16) +
				((long) byteArray[3] << 24) +
				((long) byteArray[4] << 32)) % AbstractNode.MOD;
	}

	/* Initialize input stream from socket. */
	static BufferedReader getInputStream(Socket s) throws IOException {
		InputStream sis = s.getInputStream();
		InputStreamReader sisr = new InputStreamReader(sis, CHARSET);
		BufferedReader sbr = new BufferedReader(sisr);
		return sbr;
	}

	/* Initialize output stream from socket. */
	static DataOutputStream getOutputStream(Socket s) throws IOException {
		OutputStream sos = s.getOutputStream();
		BufferedOutputStream sbos = new BufferedOutputStream(sos);
		DataOutputStream sdos = new DataOutputStream(sbos);
		return sdos;
	}
}
