package swiss.sib.swissprot.voidcounter;

import java.util.concurrent.Callable;

import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;

public abstract class QueryCallable<T> implements Callable<Exception> {
	private static final int DEFAULT_SLEEP = 60*1000;
	private static final int MAX_ATTEMPTS = 3;
	protected static final long SWITCH_TO_OPTIMIZED_COUNT_AT = 100_000_000L;
	protected final CommonVariables cv;
	protected volatile boolean running = false;
	private int attempt = 0;
	private volatile String query;
	private Exception exception;

	protected QueryCallable(CommonVariables cv) {
		super();
		this.cv = cv;
	}

	@Override
	public Exception call() {
		try {
			cv.limiter().acquireUninterruptibly();
			while (attempt < MAX_ATTEMPTS)
				try (RepositoryConnection localConnection = cv.repository().getConnection()) {
					execute(localConnection);
					return null;
				} catch (QueryEvaluationException e) {
					//If we ran out of resources we wait a bit and try again a little bit later.
					getLog().debug("Failed due to: {} at attempt {}", e.getMessage(), attempt);
					exception = e;
					++attempt;
					int sleepInMilliSeconds = DEFAULT_SLEEP;
					if (e.getMessage().contains("allocate")) {
						//not enough ram for qlever sleep longer
						sleepInMilliSeconds = DEFAULT_SLEEP * 5;
					}
					try {
						Thread.sleep(sleepInMilliSeconds);
					} catch (InterruptedException e1) {
						Thread.interrupted();
						return e;
					}
				} catch (Exception e) {
					logFailed(e);
					return e;
				}
		} finally {
			running = false;
			cv.limiter().release();
		}
		return exception;
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

	protected abstract Logger getLog();
}
