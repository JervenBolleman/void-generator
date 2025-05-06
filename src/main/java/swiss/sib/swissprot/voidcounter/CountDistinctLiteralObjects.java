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

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.ServiceDescription;
import swiss.sib.swissprot.servicedescription.sparql.Helper;
import virtuoso.rdf4j.driver.VirtuosoRepositoryConnection;

public final class CountDistinctLiteralObjects extends QueryCallable<Long> {
	private final GraphDescription gd;
	private static final Logger log = LoggerFactory.getLogger(CountDistinctLiteralObjects.class);
	private final ServiceDescription sd;
	private final Consumer<ServiceDescription> saver;
	private final String graphname;
	private final Lock writeLock;

	public CountDistinctLiteralObjects(GraphDescription gd, ServiceDescription sd, Repository repository,
			Consumer<ServiceDescription> saver, Lock writeLock, Semaphore limiter,
			AtomicInteger finishedQueries) {
		super(repository, limiter, finishedQueries);
		this.gd = gd;
		this.sd = sd;
		this.saver = saver;
		this.writeLock = writeLock;
		this.graphname = gd.getGraphName();
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct literal objects for {}",  graphname);
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("failed counting distinct literal objects for {}", graphname, e);

	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct literal {} objects for {}", gd.getDistinctLiteralObjectCount(), graphname);
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
		setQuery("SELECT (count(?object) AS ?objects) WHERE { {SELECT DISTINCT ?object { GRAPH <" + graphname
				+ "> {?subject ?predicate ?object . FILTER (!(isIRI(?object))|| !isBlank(?object)))}}} }");
		return Helper.getSingleLongFromSparql(getQuery(), connection, "objects");
	}

	private Long virtuosoOptimized(RepositoryConnection connection) {
		Connection vrc = ((VirtuosoRepositoryConnection) connection).getQuadStoreConnection();
		setQuery("SELECT COUNT(DISTINCT(RDF_QUAD.O)) FROM RDF_QUAD WHERE RDF_QUAD.G = iri_to_id('"
				+ gd.getGraphName() + "') AND RDF_IS_LITERAL(RDF_QUAD.O) = 1");
		long countOfLiterals = 0;
		try (java.sql.Statement stat = vrc.createStatement()) {
			try (ResultSet res = stat.executeQuery(getQuery())) {
				log.debug("Counting literal objects for " + graphname);
				while (res.next()) {
					countOfLiterals = res.getLong(1);
				}
				log.debug("Counted " + countOfLiterals + " literal objects for " + graphname);
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
			gd.setDistinctLiteralObjectCount(count);
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