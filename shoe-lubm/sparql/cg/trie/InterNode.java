/**
 * 
 */
package cg.trie;

import java.nio.ByteBuffer;

import cg.cache.Block;
import cg.cache.BlockHeader;

/**
 * @author C_G
 *
 */
public class InterNode extends Block {
	public static int TYPE = 1;
	private static int capacity = 95;
	public static int first = 32;
	private byte key;
	private long[] childs;
	public InterNode() {
		childs = new long[capacity];
		for (int i = 0; i < capacity; i++) {
			childs[i] = -1;
		}
		header.headType(TYPE);
	}
	public InterNode(byte key) {
		this();
		this.key = key;		
	}
	public InterNode(BlockHeader header) {
		this();
		this.header = header;
	}
	public byte key() {
		return key;
	}
	public long child(byte key) {
		return childs[key - first];
	}
	public void child(byte key, long child) {
		childs[key - first] = child;
	}
	@Override
	public void serialize(ByteBuffer buffer) {
		buffer.put(key);
		for (long child : childs)
			buffer.putLong(child);
	}

	@Override
	public void deserialize(ByteBuffer buffer) {
		key = buffer.get();
		for (int i = 0; i < capacity; i++) {
			childs[i] = buffer.getLong();
		}
	}

	@Override
	public int length() {
		return BlockHeader.SIZE + (Byte.SIZE + Long.SIZE * capacity) / 8;
	}

	@Override
	public int type() {
		return TYPE;
	}
	public String toString() {
		return "type: " + type() + ", key: " + (char)key + ", " + super.toString() + allChilds();
	}
	
	private String allChilds() {
		String ret = "";
		String tmp = "";
		for (int i = 0; i < capacity; i++) {
			if (-1 != childs[i]) {
				tmp = " childs[" + (char)(i + first) + "]=" + childs[i];
				ret += tmp;
			}
		}
		return ret;
	}
}
