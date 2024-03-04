package swiss.sib.swissprot.voidcounter;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.GraphDescription;

public class TripleCount extends QueryCallable<Long> {
	private final GraphDescription gd;
	private static final Logger log = LoggerFactory.getLogger(TripleCount.class);
	private final Lock writeLock;
	private final AtomicInteger finishedQueries;

	public TripleCount(GraphDescription gd, Repository repository, Lock writeLock, Semaphore limiter, AtomicInteger scheduledQueries, AtomicInteger finishedQueries) {
		super(repository, limiter);
		this.gd = gd;
		this.writeLock = writeLock;
		this.finishedQueries = finishedQueries;
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
		TupleQuery pq = connection.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT (COUNT(*) AS ?count) WHERE { GRAPH <"+gd.getGraphName()+"> {?s ?p ?o}}");
		try (TupleQueryResult qr = pq.evaluate()){
			if (qr.hasNext()) {
				long size = ((Literal) qr.next().getBinding("count")).longValue();
				return size;
			}
		} finally {
			finishedQueries.incrementAndGet();
		}
		return 0L;
	}

	protected void set(Long size) {
		try {
			writeLock.lock();
			gd.setTripleCount(size);
		} finally {
			writeLock.unlock();
		}
	}
}