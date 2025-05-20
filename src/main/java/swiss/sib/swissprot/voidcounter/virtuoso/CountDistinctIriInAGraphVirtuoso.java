package swiss.sib.swissprot.voidcounter.virtuoso;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.voidcounter.CommonGraphVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;
import virtuoso.rdf4j.driver.VirtuosoRepositoryConnection;

abstract class CountDistinctIriInAGraphVirtuoso extends QueryCallable<Long, CommonGraphVariables> {

	private final Consumer<Long> allSetter;
	private final BiConsumer<GraphDescription, Long> graphSetter;
	private final Map<String, Roaring64NavigableMap> graphIriIds;

	protected CountDistinctIriInAGraphVirtuoso(CommonGraphVariables cv, Consumer<Long> allSetter,
			BiConsumer<GraphDescription, Long> graphSetter, Map<String, Roaring64NavigableMap> graphIriIds2) {
		super(cv);
		this.allSetter = allSetter;
		this.graphSetter = graphSetter;
		this.graphIriIds = graphIriIds2;
	}

	protected Roaring64NavigableMap findUniqueIriIds(final Connection quadStoreConnection) {
		Roaring64NavigableMap roaringBitmap = new Roaring64NavigableMap();
		try (final Statement createStatement = quadStoreConnection.createStatement()) {
			createStatement.setFetchDirection(ResultSet.FETCH_FORWARD);
			createStatement.setFetchSize(4 * 1024);
			extractUniqueIRIIdsPerGraph(createStatement, roaringBitmap);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return roaringBitmap;
	}

	@Override
	protected Long run(RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		assert connection instanceof VirtuosoRepositoryConnection;
		// See http://docs.openlinksw.com/virtuoso/rdfiriidtype/
		final Connection quadStoreConnection = ((VirtuosoRepositoryConnection) connection).getQuadStoreConnection();

		// If we did not save this data before recalculate it.
		Roaring64NavigableMap rb;
		String graphIri = cv.gd().getGraphName();
		if (!graphIriIds.containsKey(graphIri) || graphIriIds.get(graphIri).isEmpty()) {
			rb = findUniqueIriIds(quadStoreConnection);
			graphIriIds.put(graphIri, rb);
		} else {
			rb = graphIriIds.get(graphIri);
		}
		setAll();
		return rb.getLongCardinality();
	}

	protected void extractUniqueIRIIdsPerGraph(final Statement createStatement, Roaring64NavigableMap roaringBitmap)
			throws SQLException {

		setQuery(queryForGraph());
		int i = 0, j = 0;

		long[] temp = new long[1024 * 16];
		long previous = 0;
		try (ResultSet rs = createStatement.executeQuery(getQuery())) {
			if (rs.next()) {
				previous = rs.getLong(1);
				// roaringBitmap.add(previous);
				temp[i++] = previous;
			}
			while (rs.next()) {
				long iriId = rs.getLong(1);
				if (previous != iriId) {
					temp[i++] = iriId;
					previous = iriId;
					if (i == temp.length) {
						i = 0;
						addTemp(roaringBitmap, temp);
						if (j++ == 10_000) {
							roaringBitmap.runOptimize();
						}
					}
				}
			}
		}
		addTemp(roaringBitmap, Arrays.copyOf(temp, i));
		roaringBitmap.runOptimize();
	}

	protected void addTemp(Roaring64NavigableMap roaringBitmap, long[] temp) {
		Roaring64NavigableMap rbm = new Roaring64NavigableMap();
		rbm.add(temp);
		rbm.runOptimize();
		roaringBitmap.or(rbm);
	}

	protected abstract String queryForGraph();

	@Override
	protected void set(Long count) {
		try {
			cv.writeLock().lock();

			final GraphDescription graph = cv.gd();
			if (graph != null) {
				graphSetter.accept(graph, count);
			}
		} finally {
			cv.writeLock().unlock();
		}
		cv.save();
	}

	protected void setAll() {
		try {
			Roaring64NavigableMap all = new Roaring64NavigableMap();
			for (Roaring64NavigableMap rbm : graphIriIds.values()) {
				all.or(rbm);
			}
			final long iricounts = all.getLongCardinality();
			cv.writeLock().lock();
			allSetter.accept(iricounts);
		} finally {
			cv.writeLock().unlock();
		}
	}

}