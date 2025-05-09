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

final class CountDistinctIriObjectsInDefaultGraph
    extends QueryCallable<Long>
{
	private static final String OBJECTS = "objects";
	private static final String COUNT_DISTINCT_IRI_OBJECTS_QUERY = Helper.loadSparqlQuery("count_distinct_iri_objects");
	private static final Logger log = LoggerFactory.getLogger(CountDistinctIriObjectsInDefaultGraph.class);
	private final CommonVariables cv;
	
	
	public CountDistinctIriObjectsInDefaultGraph(CommonVariables cv)
	{
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;
	}

	@Override
	protected void logStart()
	{
		log.debug("Counting distinct iri or bnode objects for default graph");
	}
	
	@Override
	protected void logFailed(Exception e)
	{
		if (log.isErrorEnabled())
			log.error("failed counting distinct iri or bnode objects for default graph", e);
	}

	@Override
	protected void logEnd()
	{
		log.debug("Counted distinct iri or bnode {} objects for default graph", getCount());
	}

	private long getCount() {
		return cv.sd().getDistinctIriObjectCount();
	}

	@Override
	protected Long run(RepositoryConnection connection)
	    throws RepositoryException, MalformedQueryException, QueryEvaluationException
	{
		setQuery(COUNT_DISTINCT_IRI_OBJECTS_QUERY);
		return Helper.getSingleLongFromSparql(getQuery(), connection, OBJECTS);
	}

	@Override
	protected void set(Long count)
	{
		try 
		{
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