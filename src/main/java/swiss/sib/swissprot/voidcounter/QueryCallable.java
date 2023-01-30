package swiss.sib.swissprot.voidcounter;

import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;

public abstract class QueryCallable<T> implements Callable<Exception> {
	protected static final long SWITCH_TO_OPTIMIZED_COUNT_AT = 100_000_000L;
	protected final Repository repository;
	protected final Semaphore limiter;

	public QueryCallable(Repository repository, Semaphore limiter) {
		super();
		this.repository = repository;
		this.limiter = limiter;
	}

	@Override
	public Exception call() {
		try {
			limiter.acquireUninterruptibly();
			try (RepositoryConnection localConnection = repository.getConnection()) {
				logStart();
				T t = run(localConnection);
				set(t);
				logEnd();
			} catch (Exception e) {
				logFailed(e);
				return e;
			}
		} finally {
			limiter.release();
		}
		return null;
	}

	protected void logFailed(Exception e) {

	}

	protected abstract void logStart();

	protected abstract void logEnd();

	protected abstract T run(RepositoryConnection connection) throws Exception;

	protected abstract void set(T t);
}