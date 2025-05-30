package swiss.sib.swissprot.voidcounter.sparql;

import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

final class CountDistinctBnodeSubjectsInDefaultGraph extends QueryCallable<Long, CommonVariables> {
	private static final String SUBJECTS = "subjects";
	private final String rawQuery;
	private static final Logger log = LoggerFactory.getLogger(CountDistinctBnodeSubjectsInDefaultGraph.class);

	public CountDistinctBnodeSubjectsInDefaultGraph(CommonVariables cv, OptimizeFor optimizeFor) {
		super(cv);
		rawQuery = Helper.loadSparqlQuery("count_distinct_bnode_subjects", optimizeFor);
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct bnode subjects for Default graph");
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct bnode subjects for Default graph");
	}

	@Override
	protected Long run(RepositoryConnection connection)
			throws QueryEvaluationException, RepositoryException, MalformedQueryException

	{
		setQuery(rawQuery);
		return Helper.getSingleLongFromSparql(getQuery(), connection, SUBJECTS);
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("Failed to run query to count distinct BNode subjects in default graph", e);
	}

	@Override
	protected void set(Long count) {
		try {
			cv.writeLock().lock();
			cv.sd().setDistinctBnodeSubjectCount(count);
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
