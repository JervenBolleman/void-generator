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

public final class CountDistinctLiteralObjects extends QueryCallable<Long> {
	private static final Logger log = LoggerFactory.getLogger(CountDistinctLiteralObjects.class);
	private final CommonVariables cv;

	public CountDistinctLiteralObjects(CommonVariables cv) {
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct literal objects for {}", cv.gd().getGraphName());
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("failed counting distinct literal objects for {}", cv.gd().getGraphName(), e);

	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct literal {} objects for {}", cv.gd().getDistinctLiteralObjectCount(),
				cv.gd().getGraphName());
	}

	@Override
	protected Long run(RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		if (connection instanceof VirtuosoRepositoryConnection) {
			return virtuosoOptimized(connection);
		} else {
			throw new IllegalStateException("Not a Virtuoso connection");
		}
	}

	private Long virtuosoOptimized(RepositoryConnection connection) {
		Connection vrc = ((VirtuosoRepositoryConnection) connection).getQuadStoreConnection();
		setQuery("SELECT COUNT(DISTINCT(RDF_QUAD.O)) FROM RDF_QUAD WHERE RDF_QUAD.G = iri_to_id('"
				+ cv.gd().getGraphName() + "') AND RDF_IS_LITERAL(RDF_QUAD.O) = 1");
		long countOfLiterals = 0;
		try (java.sql.Statement stat = vrc.createStatement()) {
			try (ResultSet res = stat.executeQuery(getQuery())) {
				log.debug("Counting literal objects for {}",cv.gd().getGraphName());
				while (res.next()) {
					countOfLiterals = res.getLong(1);
				}
				log.debug("Counted {} literal objects for {}", countOfLiterals, cv.gd().getGraphName());
				return countOfLiterals;
			} catch (SQLException e) {
				log.debug("Failed to query error:" + e.getMessage());
				throw new MalformedQueryException(e);
			}
		} catch (SQLException e) {
			log.debug("Failed to count" + e.getMessage());
			throw new RepositoryException(e);
		}
	}

	@Override
	protected void set(Long count) {
		try {
			cv.writeLock().lock();
			cv.gd().setDistinctLiteralObjectCount(count);
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