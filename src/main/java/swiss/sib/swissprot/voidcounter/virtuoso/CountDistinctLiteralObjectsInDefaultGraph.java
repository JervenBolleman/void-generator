package swiss.sib.swissprot.voidcounter.virtuoso;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;
import virtuoso.rdf4j.driver.VirtuosoRepositoryConnection;

final class CountDistinctLiteralObjectsInDefaultGraph extends QueryCallable<Long> {
	private static final String SELECT_OBJECTS_IN_RDF_OBJ = "SELECT COUNT(RO_ID) AS c FROM RDF_OBJ";
	private static final String COUNT_DISTINCT_INLINE_VALUES = "SELECT COUNT(DISTINCT(RDF_QUAD.O)) AS c from RDF_QUAD WHERE is_rdf_box(RDF_QUAD.O) = 0 AND isiri_id(O) = 0 AND is_bnode_iri_id(O) = 0";
	private static final Logger log = LoggerFactory.getLogger(CountDistinctLiteralObjectsInDefaultGraph.class);
	private final CommonVariables cv;

	public CountDistinctLiteralObjectsInDefaultGraph(CommonVariables cv) {
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct literal objects for all graphs");
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("failed counting distinct literal objects for all graphs", e);

	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct literal {} objects for all", cv.sd().getDistinctLiteralObjectCount() );
	}

	@Override
	protected Long run(RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
	
		if (connection instanceof VirtuosoRepositoryConnection vrc) {
			Connection quadConnection = vrc.getQuadStoreConnection();
			return getCount(quadConnection, COUNT_DISTINCT_INLINE_VALUES)
					+ getCount(quadConnection, SELECT_OBJECTS_IN_RDF_OBJ);
		} else {
			throw new IllegalStateException("Connection is not VirtuosoRepositoryConnection");
		}
	}

	private Long getCount(Connection vrc, String countOfInlineQuery) {
		try (java.sql.Statement stat = vrc.createStatement()) {
			setQuery(countOfInlineQuery);
			try (ResultSet res = stat.executeQuery(getQuery())) {
				log.debug("Counting literal objects for all graphs");
				long countOfLiterals = 0;
				while (res.next()) {
					countOfLiterals = res.getLong(1);
				}
				log.debug("Counted {} literal objects for all graphs", countOfLiterals );
				return countOfLiterals;
			} catch (SQLException e) {
				String msg = "Failed to query error:" + e.getMessage();
				log.debug(msg);
				throw new MalformedQueryException(msg, e);
			}
		} catch (SQLException e) {
			String msg = "Failed to count" + e.getMessage();
			log.debug(msg);
			throw new RepositoryException(msg, e);
		}
	}

	@Override
	protected void set(Long count) {
		try {
			cv.writeLock().lock();
			cv.sd().setDistinctLiteralObjectCount(count);
		} finally {
			cv.writeLock().unlock();
		}
		cv.save();
	}
	
	@Override
	protected Logger getLog() {
		return log;
	}
}