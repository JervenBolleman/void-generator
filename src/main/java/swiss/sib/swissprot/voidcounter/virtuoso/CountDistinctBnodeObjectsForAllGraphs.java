package swiss.sib.swissprot.voidcounter.virtuoso;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;
import virtuoso.rdf4j.driver.VirtuosoRepositoryConnection;

public final class CountDistinctBnodeObjectsForAllGraphs extends QueryCallable<Long> {
	private static final String COUNT_DISTINCT_OBJECT_BNODE_VIRT_SQL = "SELECT iri_id_num(RDF_QUAD.O), iri_id_num(RDF_QUAD.G) FROM RDF_QUAD WHERE isiri_id(RDF_QUAD.O) > 0 AND is_bnode_iri_id(RDF_QUAD.O) > 0";
	private static final Logger log = LoggerFactory.getLogger(CountDistinctBnodeObjectsForAllGraphs.class);
	private final CommonVariables cv;
	private final Map<Long, Roaring64Bitmap> graphIriIds = new HashMap<>();

	public CountDistinctBnodeObjectsForAllGraphs(CommonVariables cv) {
		super(cv.repository(), cv.limiter(),cv.finishedQueries());
		assert cv.repository() instanceof VirtuosoRepositoryConnection;
		this.cv = cv;
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct bnode objects for all graphs");
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("failed counting distinct bnode objects all graphs", e);
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct bnode for all graphs");
	}

	@Override
	protected Long run(RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		if (connection instanceof VirtuosoRepositoryConnection vrc) {
			// See http://docs.openlinksw.com/virtuoso/rdfiriidtype/
			final Connection quadStoreConnection = vrc.getQuadStoreConnection();
			findUniqueBnodeIds(quadStoreConnection);
			setGraphUniqueIriCounts(quadStoreConnection);
			final long iricounts = setAll();
			graphIriIds.clear();
			return iricounts;
		} else {
			throw new IllegalStateException("Connection is not a Virtuoso connection");
		}
	}

	protected void setGraphUniqueIriCounts(final Connection quadStoreConnection) {
		try {
			cv.writeLock().lock();
			write(quadStoreConnection);
		} finally {
			cv.writeLock().unlock();
		}
		cv.save();
	}

	private void write(final Connection quadStoreConnection) {
		for (Iterator<Long> iterator = graphIriIds.keySet().iterator(); iterator.hasNext();) {
			long graphId = iterator.next();
			String graphIri = IriIdToIri.idToIri(quadStoreConnection, graphId);
			final GraphDescription graph = cv.sd().getGraph(graphIri);
			if (graph != null) {
				graph.setDistinctBnodeObjectCount(graphIriIds.get(graphId).getLongCardinality());
			} else {
				iterator.remove();
			}
		}
	}

	protected long setAll() {
		Roaring64Bitmap all = new Roaring64Bitmap();
		for (Roaring64Bitmap rbm : graphIriIds.values()) {
			all.or(rbm);
		}
		final long iricounts = all.getLongCardinality();
		try {
			cv.writeLock().lock();
			cv.sd().setDistinctBnodeObjectCount(iricounts);
		} finally {
			cv.writeLock().unlock();
		}
		cv.save();
		return iricounts;
	}

	protected void findUniqueBnodeIds(final Connection quadStoreConnection) {
		try (final Statement createStatement = quadStoreConnection.createStatement()) {
			extractUniqueIRIIdsPerGraph(createStatement);
		} catch (SQLException e) {
			log.error("Counting unique BNode ids encountered an issue", e);
		}
	}

	protected void extractUniqueIRIIdsPerGraph(final Statement createStatement) throws SQLException {
		setQuery(COUNT_DISTINCT_OBJECT_BNODE_VIRT_SQL);
		try (ResultSet rs = createStatement.executeQuery(getQuery())) {
			while (rs.next()) {
				long iriId = rs.getLong(1);
				long graphId = rs.getLong(2);
				Roaring64Bitmap roaringBitmap = graphIriIds.get(graphId);
				if (roaringBitmap == null) {
					roaringBitmap = new Roaring64Bitmap();
					graphIriIds.put(graphId, roaringBitmap);
				}
				roaringBitmap.add(iriId);
			}
		}
	}

	@Override
	protected void set(Long count) {
		cv.save();
	}
	
	@Override
	protected Logger getLog() {
		return log;
	}
}