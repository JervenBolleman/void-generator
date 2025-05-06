package swiss.sib.swissprot.voidcounter;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

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
SELECT (COUNT(?subject) AS ?subjects) WHERE {
	  {
	    SELECT DISTINCT ?subject 
	    WHERE {
	      ?subject ?predicate ?object .
	      FILTER (isBlank(?subject))
	    }
	  }
	}""";
	private final GraphDescription gd;
	private final ServiceDescription sd;
	private final String graphname;
	private static final Logger log = LoggerFactory.getLogger(CountDistinctBnodeSubjects.class);
	private final Lock writeLock;
	private final Consumer<ServiceDescription> saver;
	private final boolean  defaultGraph;

	public CountDistinctBnodeSubjects(GraphDescription gd, ServiceDescription sd, Repository repository, Lock writeLock, Semaphore limiter,
			AtomicInteger finishedQueries, Consumer<ServiceDescription> saver) {
		super(repository, limiter, finishedQueries);
		this.gd = gd;
		this.writeLock = writeLock;
		this.saver = saver;
		this.sd = sd;
		this.graphname = gd.getGraphName();
		this.defaultGraph = false;
	}

	public CountDistinctBnodeSubjects(ServiceDescription sd, Repository repository, Lock writeLock, Semaphore limiter,
			AtomicInteger finishedQueries, Consumer<ServiceDescription> saver) {
		super(repository, limiter, finishedQueries);
		this.writeLock = writeLock;
		this.saver = saver;
		this.gd = null;
		this.sd = sd;
		this.graphname = "all";
		this.defaultGraph = true;
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
		if (!defaultGraph)
			return countDistinctBnodeSubjectsInGraph(connection);
		else
			return countDistinctBnodeSubjects(connection);	
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
		setQuery("SELECT (count(distinct ?subject) AS ?subjects) WHERE {"
				+ "GRAPH <"+ gd.getGraphName() + "> {?subject ?predicate ?object . FILTER(isBlank(?subject))}}");
		return Helper.getSingleLongFromSparql(getQuery(), connection, SUBJECTS);
	}

	protected Long virtuosoCountDistinctBnodeSubjectsInGraph(RepositoryConnection connection) {
		setQuery("SELECT iri_id_num(RDF_QUAD.S) FROM RDF_QUAD WHERE RDF_QUAD.G = iri_to_id('"
				+ gd.getGraphName() + "') AND is_bnode_iri_id(RDF_QUAD.S) > 0");
		return VirtuosoFromSQL.countDistinctLongResultsFromVirtuoso(connection, getQuery());
	}

	private Long countDistinctBnodeSubjects(RepositoryConnection connection) {
		if (connection instanceof VirtuosoRepositoryConnection) {
			// See http://docs.openlinksw.com/virtuoso/rdfiriidtype/
			// Plus trick from sqlbif.c
			setQuery("SELECT iri_id_num(RDF_QUAD.S) FROM RDF_QUAD WHERE is_bnode_iri_id(RDF_QUAD.S) > 0");
			return VirtuosoFromSQL.countDistinctLongResultsFromVirtuoso(connection, getQuery());
		} else {
			setQuery(COUNT_DISTINCT_SUBJECT_QUERY);
			return Helper.getSingleLongFromSparql(getQuery(), connection, SUBJECTS);
		}
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("Failed to run query to count distinct BNode subjects", e);
	}

	@Override
	protected void set(Long count) {
		if (!defaultGraph)
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
		if (sd != null) {
			saver.accept(sd);
		}
	}
	
	@Override
	protected Logger getLog() {
		return log;
	}
}
