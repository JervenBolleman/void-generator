package swiss.sib.swissprot.servicedescription;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
/**
 * Use to store internal ids of virtuoso objects in a bitmap.
 */
public class BitMapHelper {
	private BitMapHelper() {
		// private constructor to prevent instantiation
	}

	static ConcurrentHashMap<String, Roaring64NavigableMap> readGraphsWithSerializedBitMaps(File file, boolean forcedRefresh, Logger log) {
		ConcurrentHashMap<String, Roaring64NavigableMap> map = new ConcurrentHashMap<>();
		if (file.exists() && !forcedRefresh) {
			try (FileInputStream fis = new FileInputStream(file); ObjectInputStream bis = new ObjectInputStream(fis)) {
				int numberOfGraphs = bis.readInt();
				for (int i = 0; i < numberOfGraphs; i++) {
					String graph = bis.readUTF();
					Roaring64NavigableMap rb = new Roaring64NavigableMap();
					rb.readExternal(bis);
					map.put(graph, rb);
				}
			} catch (IOException e) {
				log.error("IO error", e);
			} catch (ClassNotFoundException e) {
				log.error("Class can't be found code out of sync", e);
			}
		}
		return map;
	}

	static void writeGraphsWithSerializedBitMaps(File targetFile,
			ConcurrentHashMap<String, Roaring64NavigableMap> map, Logger log) {
		if (!targetFile.exists()) {
			try {
				if (!targetFile.createNewFile()) {
					throw new RuntimeException("Can't create file we need later");
				}
			} catch (IOException e) {
				throw new RuntimeException("Can't create file we need later:", e);
			}
		}
		try (FileOutputStream fos = new FileOutputStream(targetFile);
				BufferedOutputStream bos = new BufferedOutputStream(fos);
				ObjectOutputStream dos = new ObjectOutputStream(bos)) {
			dos.writeInt(map.size());
			for (Map.Entry<String, Roaring64NavigableMap> en : map.entrySet()) {
				dos.writeUTF(en.getKey());
				Roaring64NavigableMap value = en.getValue();
				value.runOptimize();
				value.writeExternal(dos);
			}
	
		} catch (IOException e) {
			log.error("IO issue", e);
		}
	}
}
