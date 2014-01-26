/**
 * 
 */
package cg.cache;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author C_G
 *
 */
public abstract class LRU<K, V> extends LinkedHashMap<K, V>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected int capacity;
	protected LRU(int size) {
		super(size + 1);
		this.capacity = size;		
	}
	@Override 
	protected boolean removeEldestEntry(Map.Entry<K, V> eldst) {
		return removeOldEntry(eldst);
	}
	
	protected abstract boolean removeOldEntry(Map.Entry<K, V> eldst);

}
