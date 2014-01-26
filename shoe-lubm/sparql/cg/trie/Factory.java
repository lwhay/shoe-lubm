/**
 * 
 */
package cg.trie;

import cg.cache.Block;
import cg.cache.BlockHeader;

/**
 * @author C_G
 *
 */
public class Factory {
	public static Block createNode(BlockHeader header) {
		if (header.headType() == LeafNode.TYPE)
			return new LeafNode(header);
		if (header.headType() == InterNode.TYPE)
			return new InterNode(header);
		return null;
	}
}
