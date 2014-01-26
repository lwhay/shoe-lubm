/**
 * 
 */
package cg.trie;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import cg.cache.Block;
import cg.cache.BlockHeader;
import cg.cache.Cache;


/**
 * @author C_G
 *
 */
public class TrieCache extends Cache {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3145113650164968282L;
	protected TrieCache(int size) {
		super(size);
	}
	public TrieCache(RandomAccessFile file, int size, int bsize) {
		super(file, size, bsize);
	}
	public Block get(long seek) {
		Block blk = null;
		
		if (seek < filelength) {
			blk = super.get(seek);
			
			if (blk == null) {
				BlockHeader header = new BlockHeader();
				try {
//					ByteBuffer buff = fchannel.map(MapMode.READ_WRITE, seek, Block.SIZE);
//					header.deserializer(buff);
					file.seek(seek);
					header.deserializer(file);
					blk = Factory.createNode(header);
					byte[] bytes = new byte[blk.length()];
					file.read(bytes);
					ByteBuffer buff = ByteBuffer.wrap(bytes);
					blk.deserialize(buff);
				} catch (IOException e) {
					e.printStackTrace();
				}
				super.put(blk.seek(), blk);
			}
		} else {
			blk = buffer.get(seek);
		}
		
		return blk;
	}
}
