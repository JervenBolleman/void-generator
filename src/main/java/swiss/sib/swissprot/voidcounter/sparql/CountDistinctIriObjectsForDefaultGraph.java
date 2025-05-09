package swiss.sib.swissprot.voidcounter.sparql;

import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;
import virtuoso.jdbc4.VirtuosoConnection;

final class CountDistinctIriObjectsForDefaultGraph extends QueryCallable<Long> {
	private static final String OBJECTS = "objects";

	private static final String COUNT_DISTINCT_OBJECT_IRI_QUERY = Helper.loadSparqlQuery("count_distinct_iri_objects");

	private static final Logger log = LoggerFactory.getLogger(CountDistinctIriObjectsForDefaultGraph.class);

	private final CommonVariables cv;

	public CountDistinctIriObjectsForDefaultGraph(CommonVariables cv) {
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct iri objects for all graphs");
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("failed counting distinct iri objects", e);
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct iri");
	}

	@Override
	protected Long run(RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {

		assert !(connection instanceof VirtuosoConnection);
	
		setQuery(COUNT_DISTINCT_OBJECT_IRI_QUERY);
		return Helper.getSingleLongFromSparql(getQuery(), connection, OBJECTS);
	}

	@Override
	protected void set(Long count) {
		try {
			cv.writeLock().lock();
			cv.sd().setDistinctIriObjectCount(count);
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