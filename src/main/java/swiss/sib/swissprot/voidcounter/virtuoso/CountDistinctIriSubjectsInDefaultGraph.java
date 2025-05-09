package swiss.sib.swissprot.voidcounter.virtuoso;

import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;
import virtuoso.rdf4j.driver.VirtuosoRepositoryConnection;

final class CountDistinctIriSubjectsInDefaultGraph extends QueryCallable<Long> {

	private static final Logger log = LoggerFactory.getLogger(CountDistinctIriSubjectsInDefaultGraph.class);
	private CommonVariables cv;

	public CountDistinctIriSubjectsInDefaultGraph(CommonVariables cv) {
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;
		
	}

	private long countDistinctSubjectsInAllGraphs(RepositoryConnection localConnection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		if (localConnection instanceof VirtuosoRepositoryConnection vrc) {
			// See http://docs.openlinksw.com/virtuoso/rdfiriidtype/
			setQuery(
					"SELECT iri_id_num(RDF_QUAD.S) FROM RDF_QUAD WHERE isiri_id(RDF_QUAD.S) > 0 AND is_bnode_iri_id(RDF_QUAD.S) = 0");
			return VirtuosoFromSQL.countDistinctLongResultsFromVirtuoso(vrc, getQuery());
		} else {
			throw new IllegalStateException("Connection is not a Virtuoso connection");
		}
	}

	@Override
	protected void logFailed(Exception e) {
		if (log.isErrorEnabled()) {
			log.error("Failed to run query to count distinct IRI subjects for all graphs", e);
		}
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct iri subjects for all graphs");
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct iri subjects for for all graphs");
	}

	@Override
	protected Long run(RepositoryConnection connection) throws Exception {
		return countDistinctSubjectsInAllGraphs(connection);
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