package swiss.sib.swissprot.voidcounter;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.ServiceDescription;
import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.virtuoso.VirtuosoFromSQL;
import virtuoso.rdf4j.driver.VirtuosoRepositoryConnection;

public final class CountDistinctBnodeSubjects extends QueryCallable<Long> {
	private static final String SUBJECTS = "subjects";
	private static final String COUNT_DISTINCT_SUBJECT_QUERY = """
			SELECT 
				(count(distinct ?subject) AS ?subjects) 
			WHERE {
				?subject ?predicate ?object . 
				FILTER(isBlank(?subject))
			}""";
	private final GraphDescription gd;
	private final ServiceDescription sd;
	private final String graphname;
	private static final Logger log = LoggerFactory.getLogger(CountDistinctBnodeSubjects.class);
	private final Lock writeLock;
	private final AtomicInteger finishedQueries;

	public CountDistinctBnodeSubjects(GraphDescription gd, Repository repository, Lock writeLock, Semaphore limiter,
			AtomicInteger scheduledQueries, AtomicInteger finishedQueries) {
		super(repository, limiter);
		this.gd = gd;
		this.writeLock = writeLock;
		this.finishedQueries = finishedQueries;
		this.sd = null;
		this.graphname = gd.getGraphName();
		scheduledQueries.incrementAndGet();
	}

	public CountDistinctBnodeSubjects(ServiceDescription sd, Repository repository, Lock writeLock, Semaphore limiter,
			AtomicInteger scheduledQueries, AtomicInteger finishedQueries) {
		super(repository, limiter);
		this.writeLock = writeLock;
		this.finishedQueries = finishedQueries;
		this.gd = null;
		this.sd = sd;
		this.graphname = "all";
		scheduledQueries.incrementAndGet();
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
		try {
			if (gd != null)
				return countDistinctBnodeSubjectsInGraph(connection);
			else
				return countDistinctBnodeSubjects(connection);
		} finally {
			finishedQueries.incrementAndGet();
		}
	}

	private Long countDistinctBnodeSubjectsInGraph(RepositoryConnection connection) {
		if (connection instanceof VirtuosoRepositoryConnection) {
			// See http://docs.openlinksw.com/virtuoso/rdfiriidtype/
			// Plus trick from sqlbif.c
			return virtuosoCountDistinctBnodeSubjectsInGraph(connection);
		} else {
			return pureSparqlCountDistinctBnodeSubjectsInGraph(connection);
		}
	}

	protected Long pureSparqlCountDistinctBnodeSubjectsInGraph(RepositoryConnection connection) {
		query = "SELECT (count(distinct ?subject) AS ?subjects) WHERE {"
				+ "GRAPH <"+ gd.getGraphName() + "> {?subject ?predicate ?object . FILTER(isBlank(?subject))}}";
		return Helper.getSingleLongFromSparql(query, connection, SUBJECTS);
	}

	protected Long virtuosoCountDistinctBnodeSubjectsInGraph(RepositoryConnection connection) {
		query = "SELECT iri_id_num(RDF_QUAD.S) FROM RDF_QUAD WHERE RDF_QUAD.G = iri_to_id('"
				+ gd.getGraphName() + "') AND is_bnode_iri_id(RDF_QUAD.S) > 0";
		return VirtuosoFromSQL.countDistinctLongResultsFromVirtuoso(connection, query);
	}

	private Long countDistinctBnodeSubjects(RepositoryConnection connection) {
		if (connection instanceof VirtuosoRepositoryConnection) {
			// See http://docs.openlinksw.com/virtuoso/rdfiriidtype/
			// Plus trick from sqlbif.c
			query = "SELECT iri_id_num(RDF_QUAD.S) FROM RDF_QUAD WHERE is_bnode_iri_id(RDF_QUAD.S) > 0";
			return VirtuosoFromSQL.countDistinctLongResultsFromVirtuoso(connection, query);
		} else {
			query = COUNT_DISTINCT_SUBJECT_QUERY;
			return Helper.getSingleLongFromSparql(COUNT_DISTINCT_SUBJECT_QUERY, connection, SUBJECTS);
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
