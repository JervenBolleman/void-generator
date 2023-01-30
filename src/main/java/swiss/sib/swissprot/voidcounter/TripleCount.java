package swiss.sib.swissprot.voidcounter;

import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;

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

	public TripleCount(GraphDescription gd, Repository repository, Lock writeLock, Semaphore limiter) {
		super(repository, limiter);
		this.gd = gd;
		this.writeLock = writeLock;
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
		return connection.size(connection.getValueFactory().createIRI(gd.getGraphName()));
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