package swiss.sib.swissprot.voidcounter;

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
import virtuoso.jdbc4.VirtuosoConnection;

public final class CountDistinctIriObjectsForAllGraphsAtOnce extends QueryCallable<Long> {
	private static final String OBJECTS = "objects";

	private static final String COUNT_DISTINCT_OBJECT_IRI_QUERY = """
			SELECT 
				(count(distinct(?object)) as ?objects) 
			WHERE {
				?subject ?predicate ?object . 
				FILTER (isIri(?object))
			}""";

	private static final Logger log = LoggerFactory.getLogger(CountDistinctIriObjectsForAllGraphsAtOnce.class);
	private final ServiceDescription sd;
	private final Consumer<ServiceDescription> saver;

	private final Lock writeLock;

	private final AtomicInteger finishedQueries;

	public CountDistinctIriObjectsForAllGraphsAtOnce(ServiceDescription sd, Repository repository,
			Consumer<ServiceDescription> saver, Lock writeLock, Semaphore limiter, AtomicInteger scheduledQueries, AtomicInteger finishedQueries) {
		super(repository, limiter);
		this.sd = sd;
		this.saver = saver;
		this.writeLock = writeLock;
		scheduledQueries.incrementAndGet();
		this.finishedQueries = finishedQueries;
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct iri objects for all graphs");
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("failed counting distinct iri objects", e);
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct iri");
	}

	@Override
	protected Long run(RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {

		assert !(connection instanceof VirtuosoConnection);
		try {
			query = COUNT_DISTINCT_OBJECT_IRI_QUERY;
			return Helper.getSingleLongFromSparql(COUNT_DISTINCT_OBJECT_IRI_QUERY, connection, OBJECTS);
		} finally {
			finishedQueries.incrementAndGet();
		}
	}

	@Override
	protected void set(Long count) {
		try {
			writeLock.lock();
			sd.setDistinctIriObjectCount(count);
		} finally {
			writeLock.unlock();
		}
		saver.accept(sd);
	}
}