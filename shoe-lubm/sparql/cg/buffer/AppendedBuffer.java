/**
 * 
 */
package cg.buffer;

import java.nio.ByteBuffer;
import java.util.HashMap;

import cg.cache.Block;

/**
 * @author C_G
 *
 */
public class AppendedBuffer {
	private Block[] blocks;
	private HashMap<Long, Integer> map;
	private int size;
	private int length;
	private int ptr;
	public AppendedBuffer(int size) {
		this.size = size;
		ptr = 0;
		length = 0;
		blocks = new Block[size];
		map = new HashMap<Long, Integer>(size);
		for (int i = 0; i < size; i++)
			blocks[i] = null;
	}
	public boolean contain(long seek) {
		return map.containsKey(seek);
	}
	public Block get(long seek) {
		Integer ret = map.get(seek);
		if (ret != null)
			return blocks[ret];
		return null;
	}
	public void put(Block blk) {
		blocks[ptr] = blk;
		map.put(blk.seek(), ptr);
		ptr++;
		length += blk.length();	
	}
	public byte[] getBytes() {
		ByteBuffer buffer = ByteBuffer.allocate(length);
		for (int i = 0;i < ptr; i++) {
			blocks[i].write(buffer);
		}
		if (buffer.hasArray())
			return buffer.array();
		return null;
	}
	public boolean hasNext() {
		return ptr < size;
	}
	
	public void clear() {
		ptr = 0;
		length = 0;
		map.clear();
	}
	
	public boolean isEmpty() {
		return ptr == 0;
	}
}
