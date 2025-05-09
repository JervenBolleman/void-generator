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

final class CountDistinctIriSubjectsForDefaultGraph extends QueryCallable<Long> {
	private static final String SUBJECTS = "subjects";

	private static final Logger log = LoggerFactory.getLogger(CountDistinctIriSubjectsForDefaultGraph.class);

	private final CommonVariables cv;

	public CountDistinctIriSubjectsForDefaultGraph(CommonVariables cv, OptimizeFor optimizeFor) {
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;
		setQuery(Helper.loadSparqlQuery("count_distinct_iri_subjects", optimizeFor));
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct iri subject for in the Default graph");
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("failed counting distinct iri subjects in the Default graph ", e);
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct iri subjects in the Default graph");
	}

	@Override
	protected Long run(RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		return Helper.getSingleLongFromSparql(getQuery(), connection, SUBJECTS);
	}

	@Override
	protected void set(Long count) {
		try {
			cv.writeLock().lock();
			cv.sd().setDistinctIriSubjectCount(count);
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