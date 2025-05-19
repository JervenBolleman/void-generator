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

final class CountDistinctBnodeObjectsInDefaultGraph extends QueryCallable<Long> {
	private static final Logger log = LoggerFactory.getLogger(CountDistinctBnodeObjectsInDefaultGraph.class);
	private static final String OBJECTS = "objects";

	private final String countDistinctBnodeObjectsInAllGraphs;

	public CountDistinctBnodeObjectsInDefaultGraph(CommonVariables cv, OptimizeFor optimizeFor) {
		super(cv);
		countDistinctBnodeObjectsInAllGraphs = Helper.loadSparqlQuery("count_distinct_bnode_objects_in_all_graphs",
				optimizeFor);
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct bnode objects for all graphs");
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("failed counting distinct bnode objects all graphs", e);
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct bnode for all graphs");
	}

	@Override
	protected Long run(RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		return Helper.getSingleLongFromSparql(countDistinctBnodeObjectsInAllGraphs, connection, OBJECTS);
	}

	@Override
	protected void set(Long count) {
		try {
			cv.writeLock().lock();
			cv.sd().setDistinctBnodeObjectCount(count);
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