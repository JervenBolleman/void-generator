package swiss.sib.swissprot.voidcounter.sparql;

import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

final class CountDistinctLiteralObjectsInDefaultGraph extends QueryCallable<Long, CommonVariables> {
	private static final String OBJECTS = "objects";
	private final String countObjectsWithSparql;
	private static final Logger log = LoggerFactory.getLogger(CountDistinctLiteralObjectsInDefaultGraph.class);

	public CountDistinctLiteralObjectsInDefaultGraph(CommonVariables cv, OptimizeFor optimizeFor) {
		super(cv);
		this.countObjectsWithSparql = Helper.loadSparqlQuery("count_distinct_literal_objects", optimizeFor);
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct literal objects for all graphs");
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("failed counting distinct literal objects for all graphs", e);

	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct literal {} objects for all", cv.sd().getDistinctLiteralObjectCount() );
	}

	@Override
	protected Long run(RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
	
		setQuery(countObjectsWithSparql);
		return Helper.getSingleLongFromSparql(countObjectsWithSparql, connection, OBJECTS);
	}

	@Override
	protected void set(Long count) {
		try {
			cv.writeLock().lock();
			cv.sd().setDistinctLiteralObjectCount(count);
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