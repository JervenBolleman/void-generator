package swiss.sib.swissprot.voidcounter;

import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ServiceDescription;
import swiss.sib.swissprot.virtuoso.VirtuosoFromSQL;
import virtuoso.jdbc4.VirtuosoConnection;

public final class CountDistinctIriObjectsForAllGraphs extends QueryCallable<Long> {
	private final String countDistinctObjectIriQuery = "SELECT (count(distinct(?object)) as ?types) WHERE {?subject ?predicate ?object . FILTER (isIri(?object))}";

	private static final Logger log = LoggerFactory.getLogger(CountDistinctIriObjectsForAllGraphs.class);
	private final ServiceDescription sd;
	private final Consumer<ServiceDescription> saver;

	private final Lock writeLock;

	public CountDistinctIriObjectsForAllGraphs(ServiceDescription sd, Repository repository,
			Consumer<ServiceDescription> saver, Lock writeLock, Semaphore limiter) {
		super(repository, limiter);
		this.sd = sd;
		this.saver = saver;
		this.writeLock = writeLock;
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

		return ((Literal) VirtuosoFromSQL.getFirstResultFromTupleQuery(countDistinctObjectIriQuery, connection))
				.longValue();
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