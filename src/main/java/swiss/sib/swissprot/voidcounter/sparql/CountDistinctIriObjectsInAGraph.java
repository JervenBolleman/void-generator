package swiss.sib.swissprot.voidcounter.sparql;

import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.voidcounter.CommonGraphVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

final class CountDistinctIriObjectsInAGraph extends QueryCallable<Long, CommonGraphVariables> {
	private static final String OBJECTS = "objects";
	private final String rq;
	private static final Logger log = LoggerFactory.getLogger(CountDistinctIriObjectsInAGraph.class);

	public CountDistinctIriObjectsInAGraph(CommonGraphVariables cv, OptimizeFor optimizeFor) {
		super(cv);
		assert cv.gd() != null;
		this.rq = Helper.loadSparqlQuery("count_distinct_iri_objects_in_all_graphs", optimizeFor);
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct iri or bnode objects for {}", getGraphName());
	}

	private String getGraphName() {
		return cv.gd().getGraphName();
	}

	@Override
	protected void logFailed(Exception e) {
		if (log.isErrorEnabled())
			log.error("failed counting distinct iri or bnode objects for " + getGraphName(), e);
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct iri or bnode {} objects for {} ", getCount(), getGraphName());
	}

	private long getCount() {
		return cv.gd().getDistinctIriObjectCount();
	}

	@Override
	protected Long run(RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		MapBindingSet bindings = new MapBindingSet();
		bindings.addBinding("graph", cv.gd().getGraph());
		setQuery(rq, bindings);
		return Helper.getSingleLongFromSparql(getQuery(), connection, OBJECTS);
	}

	@Override
	protected void set(Long count) {
		try {
			cv.writeLock().lock();
			cv.gd().setDistinctIriObjectCount(count);
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