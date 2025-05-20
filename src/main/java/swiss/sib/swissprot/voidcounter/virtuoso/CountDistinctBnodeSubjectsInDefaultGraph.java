package swiss.sib.swissprot.voidcounter.virtuoso;

import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;
import virtuoso.rdf4j.driver.VirtuosoRepository;
import virtuoso.rdf4j.driver.VirtuosoRepositoryConnection;

final class CountDistinctBnodeSubjectsInDefaultGraph extends QueryCallable<Long,CommonVariables> {
	private static final Logger log = LoggerFactory.getLogger(CountDistinctBnodeSubjectsInDefaultGraph.class);

	public CountDistinctBnodeSubjectsInDefaultGraph(CommonVariables cv) {
		super(cv);
		assert cv.repository() instanceof VirtuosoRepository;
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
		if (connection instanceof VirtuosoRepositoryConnection) {
			// See http://docs.openlinksw.com/virtuoso/rdfiriidtype/
			// Plus trick from sqlbif.c
			setQuery("SELECT iri_id_num(RDF_QUAD.S) FROM RDF_QUAD WHERE is_bnode_iri_id(RDF_QUAD.S) > 0");
			return VirtuosoFromSQL.countDistinctLongResultsFromVirtuoso(connection, getQuery());
		} else {
			throw new IllegalStateException("Connection is not a Virtuoso connection");
		}
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
	protected Logger getLog() {
		return log;
	}
}
