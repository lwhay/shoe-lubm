package cg.trie;


public class KeyAnalyzer {
	public static byte ENDMARK = Byte.parseByte((int)'$' + "");
	private int keylength = 0;
	private long value;
	private byte[] keys;
	public long getValue() {
		return value;
	}
	public void setValue(long value) {
		this.value = value;
	}

	private int ptr = -1;
	public KeyAnalyzer() {};
	public KeyAnalyzer(String key, long value) {
		keys = key.getBytes();
		keylength = keys.length;
		this.value = value;
		
	}

	public byte key() {
		ptr++;
		return keys[ptr];
	}
	public int next() {
		if (ptr > keylength)
			return -1;
		return ptr;
	}
	public byte getKey(int id) {
		return keys[id];
	}
	public byte current() {
		return keys[ptr];
	}
	public int length() {
		return Long.SIZE;
	}
	
	public String toString() {
		return new String(keys) + ", Value: " + value;
	}
	
	public String getKey() {
		return new String(keys, 0 ,keylength);
	}
}
