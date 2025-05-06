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

public final class CountDistinctIriSubjectsForDefaultGraph extends QueryCallable<Long> {
	private static final String SUBJECTS = "subjects";

	private static final String COUNT_DISTINCT_SUBJECT_IRI_QUERY = Helper.loadSparqlQuery("count_distinct_iri_subjects");

	private static final Logger log = LoggerFactory.getLogger(CountDistinctIriSubjectsForDefaultGraph.class);
	private final ServiceDescription sd;
	private final Consumer<ServiceDescription> saver;
	private final Lock writeLock;

	public CountDistinctIriSubjectsForDefaultGraph(ServiceDescription sd, Repository repository,
			Consumer<ServiceDescription> saver, Lock writeLock, Semaphore limiter,
			AtomicInteger finishedQueries) {
		super(repository, limiter, finishedQueries);
		this.sd = sd;
		this.saver = saver;
		this.writeLock = writeLock;
		setQuery(COUNT_DISTINCT_SUBJECT_IRI_QUERY);
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct iri subject for in the Default graph");
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("failed counting distinct iri subjects in the Default graph ", e);
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct iri subjects in the Default graph");
	}

	@Override
	protected Long run(RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {	
		assert !(connection instanceof VirtuosoRepositoryConnection);
		return Helper.getSingleLongFromSparql(getQuery(), connection, SUBJECTS);
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
	
	@Override
	protected Logger getLog() {
		return log;
	}
}