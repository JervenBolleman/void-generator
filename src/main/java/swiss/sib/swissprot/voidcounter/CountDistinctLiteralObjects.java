package swiss.sib.swissprot.voidcounter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.sparql.Helper;
import virtuoso.rdf4j.driver.VirtuosoRepositoryConnection;

public final class CountDistinctLiteralObjects extends QueryCallable<Long> {
	private static final Logger log = LoggerFactory.getLogger(CountDistinctLiteralObjects.class);
	private static final String COUNT_DISTINCT_LITERAL_OBJECTS_IN_ALL_GRAPHS = Helper
			.loadSparqlQuery("count_distinct_literal_objects_in_all_graphs");
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
			return pureSparql(connection);
		}
	}

	private Long pureSparql(RepositoryConnection connection) {

		MapBindingSet bindings = new MapBindingSet();
		bindings.addBinding("graph", cv.gd().getGraph());
		setQuery(COUNT_DISTINCT_LITERAL_OBJECTS_IN_ALL_GRAPHS, bindings);
		return Helper.getSingleLongFromSparql(getQuery(), connection, "objects");
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