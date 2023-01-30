package swiss.sib.swissprot.voidcounter;

import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.ServiceDescription;
import swiss.sib.swissprot.virtuoso.VirtuosoFromSQL;
import virtuoso.rdf4j.driver.VirtuosoRepositoryConnection;

public final class CountDistinctBnodeSubjects extends QueryCallable<Long> {
	private final GraphDescription gd;
	private final ServiceDescription sd;
	private final String graphname;
	private static final Logger log = LoggerFactory.getLogger(CountDistinctBnodeSubjects.class);
	private final Lock writeLock;

	public CountDistinctBnodeSubjects(GraphDescription gd, Repository repository, Lock writeLock, Semaphore limiter) {
		super(repository, limiter);
		this.gd = gd;
		this.writeLock = writeLock;
		this.sd = null;
		this.graphname = gd.getGraphName();
	}

	public CountDistinctBnodeSubjects(ServiceDescription sd, Repository repository, Lock writeLock, Semaphore limiter) {
		super(repository, limiter);
		this.writeLock = writeLock;
		this.gd = null;
		this.sd = sd;
		this.graphname = "all";
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct bnode subjects for " + graphname);
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct bnode subjects for " + graphname);
	}

	@Override
	protected Long run(RepositoryConnection connection)
			throws QueryEvaluationException, RepositoryException, MalformedQueryException

	{
		if (gd != null)
			return countDistinctBnodeSubjectsInGraph(connection);
		else
			return countDistinctBnodeSubjects(connection);
	}

	private Long countDistinctBnodeSubjectsInGraph(RepositoryConnection connection) {
		if (connection instanceof VirtuosoRepositoryConnection) {
			// See http://docs.openlinksw.com/virtuoso/rdfiriidtype/
			// Plus trick from sqlbif.c
			String sql = "SELECT iri_id_num(RDF_QUAD.S) FROM RDF_QUAD WHERE RDF_QUAD.G = iri_to_id('"
					+ gd.getGraphName() + "') AND is_bnode_iri_id(RDF_QUAD.S) > 0";
			return VirtuosoFromSQL.countDistinctLongResultsFromVirtuoso(connection, sql);
		} else {
			final String countDistinctSubjectQuery = "SELECT (count(distinct ?subject) AS ?types) FROM <"
					+ gd.getGraphName() + "> WHERE {?subject ?predicate ?object . FILTER(isBlank(?subject))}";
			return ((Literal) VirtuosoFromSQL.getFirstResultFromTupleQuery(countDistinctSubjectQuery, connection))
					.longValue();
		}
	}

	private Long countDistinctBnodeSubjects(RepositoryConnection connection) {
		if (connection instanceof VirtuosoRepositoryConnection) {
			// See http://docs.openlinksw.com/virtuoso/rdfiriidtype/
			// Plus trick from sqlbif.c
			String sql = "SELECT iri_id_num(RDF_QUAD.S) FROM RDF_QUAD WHERE is_bnode_iri_id(RDF_QUAD.S) > 0";
			return VirtuosoFromSQL.countDistinctLongResultsFromVirtuoso(connection, sql);
		} else {
			final String countDistinctSubjectQuery = "SELECT (count(distinct ?subject) AS ?types) WHERE {?subject ?predicate ?object . FILTER(isBlank(?subject))}";
			return ((Literal) VirtuosoFromSQL.getFirstResultFromTupleQuery(countDistinctSubjectQuery, connection))
					.longValue();
		}
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("Failed to run query to count distinct BNode subjects", e);
	}

	@Override
	protected void set(Long count) {
		if (gd != null)
			try {
				writeLock.lock();
				gd.setDistinctBnodeSubjectCount(count);
			} finally {
				writeLock.unlock();
			}
		else {
			try {
				writeLock.lock();
				sd.setDistinctBnodeSubjectCount(count);
			} finally {
				writeLock.unlock();
			}
		}
	}
}