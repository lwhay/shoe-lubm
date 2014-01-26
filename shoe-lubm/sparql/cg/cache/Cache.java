/**
 * 
 */
package cg.cache;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import cg.buffer.AppendedBuffer;

/**
 * @author C_G
 *
 */
public class Cache extends LRU<Long, Block>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected AppendedBuffer buffer;
	protected long filelength;
	protected long length;
	protected RandomAccessFile file;
	protected FileChannel fchannel;
	protected Cache(int size) {
		super(size);
	}
	public Cache(RandomAccessFile file, int size, int bsize) {
		this(size);
		this.file = file;
		fchannel = file.getChannel();
		buffer = new AppendedBuffer(bsize);
		try {
			filelength = file.length();
			length = filelength;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public void put(Block blk) {
		blk.seek(length);
		length += blk.length();
		if (buffer.hasNext()) {
			buffer.put(blk);
		}
		else {
			byte[] bytes = buffer.getBytes();
			buffer.clear();
			try {
				file.seek(filelength);
				file.write(bytes);
				filelength += bytes.length;
			} catch (IOException e) {
				e.printStackTrace();
			}
			buffer.put(blk);
		}
	}
	public void put(Block blk, Block avoid) {
		blk.seek(length);
		length += blk.length();
		if (buffer.hasNext()) {
			buffer.put(blk);
		}
		else {
			
			byte[] bytes = buffer.getBytes();
			buffer.clear();
			try {
				file.seek(filelength);
				file.write(bytes);
				filelength += bytes.length;
			} catch (IOException e) {
				e.printStackTrace();
			}
			buffer.put(blk);
			super.put(avoid.seek(), avoid);
		}
	}

	@Override
	protected boolean removeOldEntry(Map.Entry<Long, Block> eldst) {
		if (this.size() > capacity) {
			Block blk = eldst.getValue();
			if (blk.dirty()) {
				ByteBuffer buff;
				try {
					buff = fchannel.map(MapMode.READ_WRITE, blk.seek(), blk.length());
					buff.clear();
					blk.write(buff);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			return true;
		}
		return false;
	}
	
	public void close() {
		Set<Long> keys = super.keySet();
		Iterator<Long> it = keys.iterator();
		while (it.hasNext()) {
			long key = it.next();
			Block blk = super.get(key);
			if (blk.dirty()) {
				ByteBuffer buff;
				try {
					buff = fchannel.map(MapMode.READ_WRITE, blk.seek(), blk.length());
					buff.clear();
					blk.write(buff);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		if (!buffer.isEmpty()) {
			byte[] bytes = buffer.getBytes();
			buffer.clear();
			try {
				file.seek(filelength);
				file.write(bytes);
				filelength += bytes.length;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {
			fchannel.close();
			file.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
