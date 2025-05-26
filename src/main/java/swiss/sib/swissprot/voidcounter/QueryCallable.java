package swiss.sib.swissprot.voidcounter;

import java.util.concurrent.Callable;

import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;

public abstract class QueryCallable<T, C extends Variables> implements Callable<Exception> {
	protected static final long SWITCH_TO_OPTIMIZED_COUNT_AT = 100_000_000L;
	protected final C cv;
	protected volatile boolean running = false;
	private volatile String query;

	protected QueryCallable(C cv) {
		super();
		this.cv = cv;
	}

	@Override
	public Exception call() {
		try {
			cv.limiter().acquireUninterruptibly();
			try (RepositoryConnection localConnection = cv.repository().getConnection()) {
				execute(localConnection);
				return null;
			} catch (Exception e) {
				logFailed(e);
				return e;
			}
		} finally {
			running = false;
			cv.limiter().release();
		}
	}

	private final void execute(RepositoryConnection localConnection) throws Exception {
		running = true;
		logStart();
		T t = run(localConnection);
		set(t);
		logEnd();
		cv.finishedQueries().incrementAndGet();
	}

	protected void logFailed(Exception e) {
		getLog().error("Failed", e);
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

	protected void setQuery(String query) {
		this.query = query;
		getLog().debug("query: {}", query);
	}

	protected void setQuery(String dataTypeQuery, BindingSet bindings) {
		String sb = dataTypeQuery;
		for (Binding bs : bindings) {
			if (bs.getValue().isIRI()) {
				sb = sb.replace("?" + bs.getName() + " ", '<' + bs.getValue().stringValue() + "> ");
			} else {
				sb = sb.replace("?" + bs.getName() + " ", bs.getValue().stringValue()) + " ";
			}
		}
		setQuery(sb);
	}

	public abstract Logger getLog();
}
