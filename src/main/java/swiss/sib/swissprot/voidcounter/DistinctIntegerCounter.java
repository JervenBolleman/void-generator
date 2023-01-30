package swiss.sib.swissprot.voidcounter;

import java.util.HashSet;
import java.util.Set;

import org.roaringbitmap.RoaringBitmap;

public class DistinctIntegerCounter {
	public static final int CACHE_SIZE = 64 * 1048;
	private RoaringBitmap bitmap = new RoaringBitmap();
	private Set<Integer> cache = new HashSet<>(CACHE_SIZE * 2);
	private long lastOptimizedCardinality = 0;

	public void add(int isItDistinct) {
		cache.add(isItDistinct);
		if (CACHE_SIZE == cache.size()) {
			addCacheToRoarding();
			long currentCardinality = bitmap.getLongCardinality();
			if (currentCardinality - lastOptimizedCardinality > CACHE_SIZE) {
				bitmap.runOptimize();
				lastOptimizedCardinality = currentCardinality;
			}
			cache.clear();
		}
	}

	private void addCacheToRoarding() {
		RoaringBitmap rb = new RoaringBitmap();
		for (Integer i : cache) {
			rb.add(i);
		}
		bitmap.or(rb);
	}

	public long cardinality() {
		addCacheToRoarding();
		bitmap.runOptimize();
		cache.clear();
		return bitmap.getLongCardinality();
	}
}