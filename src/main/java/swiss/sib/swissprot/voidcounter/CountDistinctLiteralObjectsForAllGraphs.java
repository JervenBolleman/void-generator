package swiss.sib.swissprot.voidcounter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ServiceDescription;
import swiss.sib.swissprot.servicedescription.sparql.Helper;
import virtuoso.rdf4j.driver.VirtuosoRepositoryConnection;

public final class CountDistinctLiteralObjectsForAllGraphs extends QueryCallable<Long> {
	private static final String OBJECTS = "objects";
	private static final String COUNT_OBJECTS_WITH_SPARQL = """
			SELECT (count(distinct(?object)) AS ?objects) 
			WHERE {
				?subject ?predicate ?object .
				FILTER (isLiteral(?object))
			}""";
	private static final String SELECT_OBJECTS_IN_RDF_OBJ = "SELECT COUNT(RO_ID) AS c FROM RDF_OBJ";
	private static final String COUNT_DISTINCT_INLINE_VALUES = "SELECT COUNT(DISTINCT(RDF_QUAD.O)) AS c from RDF_QUAD WHERE is_rdf_box(RDF_QUAD.O) = 0 AND isiri_id(O) = 0 AND is_bnode_iri_id(O) = 0";
	private final static Logger log = LoggerFactory.getLogger(CountDistinctLiteralObjectsForAllGraphs.class);
	private final ServiceDescription sd;
	private final Consumer<ServiceDescription> saver;
	private final Lock writeLock;

	public CountDistinctLiteralObjectsForAllGraphs(ServiceDescription sd, Repository repository,
			Consumer<ServiceDescription> saver, Lock writeLock, Semaphore limiter,
			AtomicInteger finishedQueries) {
		super(repository, limiter, finishedQueries);
		this.sd = sd;
		this.saver = saver;
		this.writeLock = writeLock;
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
		log.debug("Counted distinct literal " + sd.getDistinctLiteralObjectCount() + "objects for all");
	}

	@Override
	protected Long run(RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
	
		if (connection instanceof VirtuosoRepositoryConnection) {
			Connection vrc = ((VirtuosoRepositoryConnection) connection).getQuadStoreConnection();
			long countOfLiterals = getCount(vrc, COUNT_DISTINCT_INLINE_VALUES)
					+ getCount(vrc, SELECT_OBJECTS_IN_RDF_OBJ);
			return countOfLiterals;
		} else {
			setQuery(COUNT_OBJECTS_WITH_SPARQL);
			return Helper.getSingleLongFromSparql(COUNT_OBJECTS_WITH_SPARQL, connection, OBJECTS);
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
				log.debug("Counted " + countOfLiterals + " literal objects for all graphs");
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
			writeLock.lock();
			sd.setDistinctLiteralObjectCount(count);
		} finally {
			writeLock.unlock();
		}
		saver.accept(sd);
	}
	
	@Override
	protected Logger getLog() {
		return log;
	}
}