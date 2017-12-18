public class Debug {
	private static final boolean _DEBUG = true;
	
	public static void DEBUG(Object o) {
		if (_DEBUG) {
			System.out.println("[DEBUG] " + o);
		}
	}
}
