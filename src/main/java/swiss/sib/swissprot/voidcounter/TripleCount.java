package swiss.sib.swissprot.voidcounter;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.ServiceDescription;
import swiss.sib.swissprot.servicedescription.sparql.Helper;

public class TripleCount extends QueryCallable<Long> {
	private final GraphDescription gd;
	private static final Logger log = LoggerFactory.getLogger(TripleCount.class);
	private final Lock writeLock;
	private final AtomicInteger finishedQueries;
	private final Consumer<ServiceDescription> saver;
	private final ServiceDescription sd;

	public TripleCount(GraphDescription gd, Repository repository, Lock writeLock, Semaphore limiter,
			AtomicInteger scheduledQueries, AtomicInteger finishedQueries, Consumer<ServiceDescription> saver,
			ServiceDescription sd) {
		super(repository, limiter);
		this.gd = gd;
		this.writeLock = writeLock;
		this.finishedQueries = finishedQueries;
		this.saver = saver;
		this.sd = sd;
		scheduledQueries.incrementAndGet();
	}

	@Override
	protected void logStart() {
		log.debug("Finding size of  " + gd.getGraphName());
	}

	@Override
	protected void logEnd() {
		log.debug("Found size of  " + gd.getGraphName());
	}

	protected Long run(RepositoryConnection connection) throws RepositoryException {
		try {
			return Helper.getSingleLongFromSparql("SELECT (COUNT(*) AS ?count) WHERE { GRAPH <" + gd.getGraphName() + "> {?s ?p ?o}}", connection, "count");			
		} finally {
			finishedQueries.incrementAndGet();
		}
	}

	protected void set(Long size) {
		try {
			writeLock.lock();
			gd.setTripleCount(size);
		} finally {
			writeLock.unlock();
		}
		saver.accept(sd);
	}
}