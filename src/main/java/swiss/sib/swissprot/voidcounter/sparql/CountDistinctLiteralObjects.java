package swiss.sib.swissprot.voidcounter.sparql;

import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

final class CountDistinctLiteralObjects extends QueryCallable<Long> {
	private static final Logger log = LoggerFactory.getLogger(CountDistinctLiteralObjects.class);
	private static final String COUNT_DISTINCT_LITERAL_OBJECTS_IN_ALL_GRAPHS = Helper
			.loadSparqlQuery("count_distinct_literal_objects_in_all_graphs");
	private final CommonVariables cv;

	public CountDistinctLiteralObjects(CommonVariables cv) {
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct literal objects for {}", cv.gd().getGraphName());
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("failed counting distinct literal objects for {}", cv.gd().getGraphName(), e);

	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct literal {} objects for {}", cv.gd().getDistinctLiteralObjectCount(),
				cv.gd().getGraphName());
	}

	@Override
	protected Long run(RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		MapBindingSet bindings = new MapBindingSet();
		bindings.addBinding("graph", cv.gd().getGraph());
		setQuery(COUNT_DISTINCT_LITERAL_OBJECTS_IN_ALL_GRAPHS, bindings);
		return Helper.getSingleLongFromSparql(getQuery(), connection, "objects");
	}

	
	@Override
	protected void set(Long count) {
		try {
			cv.writeLock().lock();
			cv.gd().setDistinctLiteralObjectCount(count);
		} finally {
			cv.writeLock().unlock();
		}
		cv.save();
	}

	@Override
	protected Logger getLog() {
		return log;
	}
}