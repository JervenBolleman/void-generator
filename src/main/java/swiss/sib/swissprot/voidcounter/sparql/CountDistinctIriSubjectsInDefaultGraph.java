package swiss.sib.swissprot.voidcounter.sparql;

import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

final class CountDistinctIriSubjectsInDefaultGraph extends QueryCallable<Long> {

	private static final String SUBJECTS = "subjects";
	private final String rawQuery;

	private static final Logger log = LoggerFactory.getLogger(CountDistinctIriSubjectsInDefaultGraph.class);
	private final CommonVariables cv;

	public CountDistinctIriSubjectsInDefaultGraph(CommonVariables cv, OptimizeFor optimizeFor) {
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;
		this.rawQuery = Helper.loadSparqlQuery("count_distinct_iri_subjects", optimizeFor);
	}

	@Override
	protected void logFailed(Exception e) {
		if (log.isErrorEnabled()) {
			log.error("Failed to run query to count distinct IRI subjects for default graph", e);
		}
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct iri subjects for default graph");
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct iri subjects for default graph");
	}

	@Override
	protected Long run(RepositoryConnection connection) throws Exception {

		setQuery(rawQuery);
		return Helper.getSingleLongFromSparql(rawQuery, connection, SUBJECTS);
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