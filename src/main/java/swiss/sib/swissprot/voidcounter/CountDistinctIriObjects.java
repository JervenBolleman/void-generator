package swiss.sib.swissprot.voidcounter;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
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
	private static final String OBJECTS = "objects";
	private static final String COUNT_DISTINCT_IRI_OBJECTS_QUERY = Helper.loadSparqlQuery("count_distinct_iri_objects");
	private static final String COUNT_DISTINCT_IRI_OBJECTS_QUERY_IN_A_GRAPH = Helper.loadSparqlQuery("count_distinct_iri_objects_in_all_graphs");
	private final GraphDescription gd;
	private static final Logger log = LoggerFactory.getLogger(CountDistinctIriObjects.class);
	private final ServiceDescription sd;
	private final Consumer<ServiceDescription> saver;
	private final Lock writeLock;
	
	
	public CountDistinctIriObjects(GraphDescription gd, ServiceDescription sd, Repository repository, Consumer<ServiceDescription> saver, Lock writeLock, Semaphore limiter, AtomicInteger finishedQueries)
	{
		super(repository, limiter, finishedQueries);
		this.gd = gd;
		this.sd = sd;
		this.saver = saver;
		this.writeLock = writeLock;
	}

	@Override
	protected void logStart()
	{
		log.debug("Counting distinct iri or bnode objects for {}", getGraphName());
	}

	private String getGraphName() {
		if (gd != null)
			return gd.getGraph().stringValue();
		else
			return "DEFAULT graph";
	}
	
	@Override
	protected void logFailed(Exception e)
	{
		if (log.isErrorEnabled())
			log.error("failed counting distinct iri or bnode objects for " + getGraphName(), e);
	}

	@Override
	protected void logEnd()
	{
		log.debug("Counted distinct iri or bnode {} objects for {} ", getCount(), getGraphName());
	}

	private long getCount() {
		if (gd != null)
			return gd.getDistinctIriObjectCount();
		else
			return sd.getDistinctIriObjectCount();
	}

	@Override
	protected Long run(RepositoryConnection connection)
	    throws RepositoryException, MalformedQueryException, QueryEvaluationException
	{
		if (connection instanceof VirtuosoRepositoryConnection vrc)
		{
			//See http://docs.openlinksw.com/virtuoso/rdfiriidtype/
			if (gd != null)
				setQuery("SELECT (COUNT(DISTINCT(iri_id_num(RDF_QUAD.O)))) FROM RDF_QUAD WHERE RDF_QUAD.G = iri_to_id('"
				    + getGraphName() + "') AND isiri_id(RDF_QUAD.O) > 0 AND is_bnode_iri_id(RDF_QUAD.O) = 0");
			else
				setQuery("SELECT (COUNT(DISTINCT(iri_id_num(RDF_QUAD.O)))) FROM RDF_QUAD AND isiri_id(RDF_QUAD.O) > 0 AND is_bnode_iri_id(RDF_QUAD.O) = 0");
			return VirtuosoFromSQL.getSingleLongFromSql(getQuery(), vrc);
		}
		else if (gd != null)
		{
			MapBindingSet bindings = new MapBindingSet();
			bindings.addBinding("graph", gd.getGraph());
			setQuery(COUNT_DISTINCT_IRI_OBJECTS_QUERY_IN_A_GRAPH, bindings);
		} else {
			setQuery(COUNT_DISTINCT_IRI_OBJECTS_QUERY);
		}
		return Helper.getSingleLongFromSparql(getQuery(), connection, OBJECTS);
	}

	@Override
	protected void set(Long count)
	{
		try 
		{
			writeLock.lock();
			if (gd != null)
				gd.setDistinctIriObjectCount(count);
			else
				sd.setDistinctIriObjectCount(count);
		} finally {
			writeLock.unlock();
		}
		saver.accept(sd);
	}
	
	@Override
	protected Logger getLog() {
		return log;
	}
}