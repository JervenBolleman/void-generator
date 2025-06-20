package swiss.sib.swissprot.voidcounter.sparql;

import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.voidcounter.CommonGraphVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

final class CountDistinctLiteralObjects extends QueryCallable<Long, CommonGraphVariables> {
	private static final Logger log = LoggerFactory.getLogger(CountDistinctLiteralObjects.class);
	private final String rawQuery;

	public CountDistinctLiteralObjects(CommonGraphVariables cv, OptimizeFor optimizeFor) {
		super(cv);
		rawQuery = Helper.loadSparqlQuery("count_distinct_literal_objects_in_all_graphs", optimizeFor);
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
		setQuery(rawQuery, bindings);
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
	public Logger getLog() {
		return log;
	}
}