package swiss.sib.swissprot.voidcounter;

import java.util.concurrent.Semaphore;
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

public final class CountDistinctIriObjects
    extends QueryCallable<Long>
{
	private static final String COUNT_DISTINCT_SUBJECT_QUERY = """
			SELECT 
				(count(distinct(?object)) as ?objects) 
			WHERE { 
				GRAPH ?graph {
					?subject ?predicate ?object . 
					FILTER (isIri(?object))
				}
			}""";
	private final GraphDescription gd;
	private static final Logger log = LoggerFactory.getLogger(CountDistinctIriObjects.class);
	private final ServiceDescription sd;
	private final Consumer<ServiceDescription> saver;
	private final Lock writeLock;
	
	
	public CountDistinctIriObjects(GraphDescription gd, ServiceDescription sd, Repository repository, Consumer<ServiceDescription> saver, Lock writeLock, Semaphore limiter)
	{
		super(repository, limiter);
		this.gd = gd;
		this.sd = sd;
		this.saver = saver;
		this.writeLock = writeLock;
	}

	@Override
	protected void logStart()
	{
		log.debug("Counting distinct iri or bnode objects for " + gd.getGraphName());
	}
	
	@Override
	protected void logFailed(Exception e)
	{
		log.error("failed counting distinct iri or bnode objects for " + gd.getGraphName(), e);
	}

	@Override
	protected void logEnd()
	{
		log.debug("Counted distinct iri or bnode " + gd.getDistinctLiteralObjectCount() + " objects for " + gd.getGraphName());
	}

	@Override
	protected Long run(RepositoryConnection connection)
	    throws RepositoryException, MalformedQueryException, QueryEvaluationException
	{
		if (connection instanceof VirtuosoRepositoryConnection)
		{
			//See http://docs.openlinksw.com/virtuoso/rdfiriidtype/
			String sql;
			if (gd != null)
				sql = "SELECT (COUNT(DISTINCT(iri_id_num(RDF_QUAD.O)))) FROM RDF_QUAD WHERE RDF_QUAD.G = iri_to_id('"
				    + gd.getGraphName() + "') AND isiri_id(RDF_QUAD.O) > 0 AND is_bnode_iri_id(RDF_QUAD.O) = 0";
			else
				sql = "SELECT (COUNT(DISTINCT(iri_id_num(RDF_QUAD.O)))) FROM RDF_QUAD AND isiri_id(RDF_QUAD.O) > 0 AND is_bnode_iri_id(RDF_QUAD.O) = 0";
			return VirtuosoFromSQL.getSingleLongFromSql(sql, (VirtuosoRepositoryConnection) connection);
		}
		else if (gd != null)
		{
			final String countDistinctSubjectQuery = "SELECT (count(distinct(?object)) as ?objects) { GRAPH <"
			    + gd.getGraphName() + "> {?subject ?predicate ?object . FILTER (isIri(?object))}}";
			return Helper.getSingleLongFromSparql(countDistinctSubjectQuery, connection, "objects");
		} else {
			
			return Helper.getSingleLongFromSparql(COUNT_DISTINCT_SUBJECT_QUERY, connection, "objects");
		}
	}

	@Override
	protected void set(Long count)
	{
		try 
		{
			writeLock.lock();
			gd.setDistinctIriObjectCount(count);
		} finally {
			writeLock.unlock();
		}
		saver.accept(sd);
	}
}