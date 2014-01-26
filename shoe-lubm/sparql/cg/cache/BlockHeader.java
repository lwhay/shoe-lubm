/**
 * 
 */
package cg.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author C_G
 *
 */
public class BlockHeader {
	public static int SIZE = (Long.SIZE + Integer.SIZE) / 8;
	private long seek;
	private int type;
	public void serializer(ByteBuffer buffer) {
		buffer.putLong(seek);
		buffer.putInt(type);
	}
	public void deserializer(ByteBuffer buffer) {
		seek = buffer.getLong();
		type = buffer.getInt();
	}
	public void serializer(DataOutput out) throws IOException {
		out.writeLong(seek);
		out.writeInt(type);
	}
	public void deserializer(DataInput in) throws IOException {
		seek = in.readLong();
		type = in.readInt();
	}
	public long seek() {
		return seek;
	}
	public long seek(long seek) {
		long old = seek();
		this.seek = seek;
		return old;
	}
	public int headType() {
		return type;
	}
	public int headType(int type) {
		int old = headType();
		this.type = type;
		return old;
	}
}
