/**
 * 
 */
package cg.cache;

import java.nio.ByteBuffer;

/**
 * @author C_G
 *
 */
public abstract class Block {
	public static int SIZE = 1000;
	protected BlockHeader header;
	protected boolean dirty = false;
	public Block() {
		header = new BlockHeader();
	}
	public Block(BlockHeader header) {
		this.header = header;
	}
	public void write(ByteBuffer buffer) {
		header.serializer(buffer);
		serialize(buffer);
	}
	public void read(ByteBuffer buffer) {
		header.deserializer(buffer);
		deserialize(buffer);
	}
	public byte[] getBytes() {
		ByteBuffer buffer = ByteBuffer.allocate(length());
		header.serializer(buffer);
		serialize(buffer);
		return buffer.array();
	}
	public long seek() {
		return header.seek();
	}
	public long seek(long seek) {
		long old = seek();
		header.seek(seek);
		return old;
	}
	public boolean dirty() {
		return dirty;
	}
	public boolean dirty(boolean dirty) {
		boolean old = dirty();
		this.dirty = dirty;
		return old;
	}
	public String toString() {
		return "Header: " + header.seek() + "," + header.headType() + ";";
	}
	public abstract void serialize(ByteBuffer buffer);
	public abstract void deserialize(ByteBuffer buffer);
	public abstract int length();
	public abstract int type();
}
