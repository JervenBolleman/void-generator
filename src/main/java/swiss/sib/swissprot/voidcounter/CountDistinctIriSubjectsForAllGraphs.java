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
import virtuoso.rdf4j.driver.VirtuosoRepositoryConnection;

public final class CountDistinctIriSubjectsForAllGraphs extends QueryCallable<Long> {
	private static final String SUBJECTS = "subjects";

	private static final String COUNT_DISTINCT_SUBJECT_IRI_QUERY = """
				SELECT 
					(count(distinct(?subject)) as ?subjects) 
				WHERE {
					?subject ?predicate ?object .
					FILTER (isIri(?subject))
				}""";

	private static final Logger log = LoggerFactory.getLogger(CountDistinctIriSubjectsForAllGraphs.class);
	private final ServiceDescription sd;
	private final Consumer<ServiceDescription> saver;
	private final Lock writeLock;
	private final AtomicInteger finishedQueries;

	public CountDistinctIriSubjectsForAllGraphs(ServiceDescription sd, Repository repository,
			Consumer<ServiceDescription> saver, Lock writeLock, Semaphore limiter,
			AtomicInteger finishedQueries) {
		super(repository, limiter);
		this.sd = sd;
		this.saver = saver;
		this.writeLock = writeLock;
		this.finishedQueries = finishedQueries;
		query = COUNT_DISTINCT_SUBJECT_IRI_QUERY;
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct iri subject for all graphs");
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("failed counting distinct iri subject ", e);
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct subject iri");
	}

	@Override
	protected Long run(RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		try {
			assert !(connection instanceof VirtuosoRepositoryConnection);
			return Helper.getSingleLongFromSparql(COUNT_DISTINCT_SUBJECT_IRI_QUERY, connection, SUBJECTS);
		} finally {
			finishedQueries.incrementAndGet();
		}

	}

	@Override
	protected void set(Long count) {
		try {
			writeLock.lock();
			sd.setDistinctIriSubjectCount(count);
		} finally {
			writeLock.unlock();
		}
		saver.accept(sd);
	}

}