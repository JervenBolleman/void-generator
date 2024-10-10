package swiss.sib.swissprot.voidcounter;

import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;

public abstract class QueryCallable<T> implements Callable<Exception> {
	protected static final long SWITCH_TO_OPTIMIZED_COUNT_AT = 100_000_000L;
	protected final Repository repository;
	protected final Semaphore limiter;
	protected volatile boolean running = false;
	protected volatile String query;
	protected final AtomicInteger finishedQueries;

	public QueryCallable(Repository repository, Semaphore limiter, AtomicInteger finishedQueries) {
		super();
		this.repository = repository;
		this.limiter = limiter;
		this.finishedQueries = finishedQueries;
	}

	@Override
	public Exception call() {
		try {
			limiter.acquireUninterruptibly();
			try (RepositoryConnection localConnection = repository.getConnection()) {
				running = true;
				logStart();
				T t = run(localConnection);
				set(t);
				logEnd();
				finishedQueries.incrementAndGet();
			} catch (Exception e) {
				logFailed(e);
				return e;
			}
		} finally {
			running = false;
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

	public final boolean isRunning() {
		return running;
	}

	public final String getQuery() {
		return query;
	}

	protected void setQuery(String dataTypeQuery, BindingSet bindings) {
		String sb = new String(dataTypeQuery);
		for (Binding bs : bindings) {
			if (bs.getValue().isIRI()) {
				sb = sb.replace("?" + bs.getName(), '<'+bs.getValue().stringValue()+'>');
			} else {
				sb = sb.replace("?" + bs.getName(), bs.getValue().stringValue());
			}
		}
		query = sb.toString();
	}
}
