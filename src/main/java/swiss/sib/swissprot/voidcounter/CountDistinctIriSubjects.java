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


public final class CountDistinctIriSubjects
    extends QueryCallable<Long>
{

	private static final String SUBJECTS = "subjects";
	private static final String COUNT_DistinctSubjectQuery = """
			SELECT 
				(COUNT(DISTINCT(?subject)) AS ?subjects) 
			WHERE {
				?subject ?predicate ?object . 
				FILTER(isIri(?s))
			}""";
	private final GraphDescription gd;
	private static final Logger log = LoggerFactory.getLogger(CountDistinctIriSubjects.class);
	private final ServiceDescription sd;
	private final String graphname;
	private final Lock writeLock;

	public CountDistinctIriSubjects(GraphDescription gd, Repository repository, Lock writeLock, Semaphore limiter, AtomicInteger finishedQueries)
	{
		super(repository, limiter, finishedQueries);
		this.gd = gd;
		this.writeLock = writeLock;
		this.sd = null;
		this.graphname = gd.getGraphName();
	}

	public CountDistinctIriSubjects(ServiceDescription sd, Repository repository, Lock writeLock, Semaphore limiter,
			AtomicInteger finishedQueries) {
		super(repository, limiter, finishedQueries);
		this.sd = sd;
		this.writeLock = writeLock;
		this.gd = null;
		this.graphname = "all";
	}

	private long countDistinctSubjects(GraphDescription gd, RepositoryConnection localConnection)
	    throws RepositoryException, MalformedQueryException, QueryEvaluationException
	{
		if (localConnection instanceof VirtuosoRepositoryConnection)
		{
			//See http://docs.openlinksw.com/virtuoso/rdfiriidtype/
			setQuery("SELECT COUNT(DISTINCT(iri_id_num(RDF_QUAD.S))) FROM RDF_QUAD WHERE RDF_QUAD.G = iri_to_id('"
			    + gd.getGraphName() + "') AND isiri_id(RDF_QUAD.S) > 0 AND is_bnode_iri_id(RDF_QUAD.S) = 0");
			return VirtuosoFromSQL.getSingleLongFromSql(query, (VirtuosoRepositoryConnection) localConnection);
		}
		else
		{
			setQuery("SELECT (COUNT(DISTINCT(?subject)) AS ?subjects) WHERE {GRAPH <"
			    + gd.getGraphName() + "> {?subject ?predicate ?object . FILTER(isIri(?s))}}");
			return Helper.getSingleLongFromSparql(query, localConnection, SUBJECTS);
		}
	}
	
	private long countDistinctSubjectsInAllGraphs(RepositoryConnection localConnection)
	    throws RepositoryException, MalformedQueryException, QueryEvaluationException
	{
		if (localConnection instanceof VirtuosoRepositoryConnection)
		{
			//See http://docs.openlinksw.com/virtuoso/rdfiriidtype/
			setQuery("SELECT iri_id_num(RDF_QUAD.S) FROM RDF_QUAD WHERE isiri_id(RDF_QUAD.S) > 0 AND is_bnode_iri_id(RDF_QUAD.S) = 0");
			return VirtuosoFromSQL.countDistinctLongResultsFromVirtuoso((VirtuosoRepositoryConnection) localConnection, query);
		}
		else
		{
			setQuery(COUNT_DistinctSubjectQuery);
			return Helper.getSingleLongFromSparql(COUNT_DistinctSubjectQuery, localConnection, SUBJECTS);
		}
	}

	@Override
	protected void logFailed(Exception e)
	{
		log.error("Failed to run query to count distinct IRI subjects for "+ graphname, e);
	}

	@Override
	protected void logStart()
	{
		log.debug("Counting distinct iri subjects for " + graphname);
	}

	@Override
	protected void logEnd()
	{
		log.debug("Counted distinct iri subjects for " + graphname);
	}

	@Override
	protected Long run(RepositoryConnection connection)
	    throws Exception
	{
		if (gd !=null)
			return countDistinctSubjects(gd, connection);
		else
		{
			return countDistinctSubjectsInAllGraphs(connection);
		}
	}

	@Override
	protected void set(Long count)
	{
		try {
			writeLock.lock();
			if (gd != null)
			{
				gd.setDistinctIriSubjectCount(count);
			}
			else
			{
				sd.setDistinctIriSubjectCount(count);
			}
		} finally {
			writeLock.unlock();
		}
	}
	
	@Override
	protected Logger getLog() {
		return log;
	}
}