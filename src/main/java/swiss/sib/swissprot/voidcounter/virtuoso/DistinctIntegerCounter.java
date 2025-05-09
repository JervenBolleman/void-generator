package swiss.sib.swissprot.voidcounter.virtuoso;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;

class DistinctIntegerCounter {
	public static final int CACHE_SIZE = 4096; // Max size of an ArrayContainer
	private RoaringBitmapWriter<RoaringBitmap> bitmap = RoaringBitmapWriter.writer().initialCapacity(CACHE_SIZE)
			.optimiseForRuns().get();
	private long lastFlushed = 0;

	public void add(int isItDistinct) {
		bitmap.add(isItDistinct);
		if (CACHE_SIZE == lastFlushed) {
			bitmap.flush();
		}
	}

	public long cardinality() {
		return bitmap.get().getLongCardinality();
	}
}