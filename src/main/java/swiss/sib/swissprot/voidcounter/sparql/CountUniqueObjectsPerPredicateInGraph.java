package swiss.sib.swissprot.voidcounter.sparql;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

class CountUniqueObjectsPerPredicateInGraph
    extends QueryCallable<Long>
{

	private static final String OBJECTS = "objects";
	private final String rawQuery;
	private static final Logger log = LoggerFactory.getLogger(CountUniqueObjectsPerPredicateInGraph.class);

	private final PredicatePartition predicatePartition;
	private final CommonVariables cv;
	
	public CountUniqueObjectsPerPredicateInGraph(CommonVariables cv, PredicatePartition predicatePartition, OptimizeFor optimizeFor)
	{
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;
		this.predicatePartition = predicatePartition;
		this.rawQuery = Helper.loadSparqlQuery("count_distinct_objects", optimizeFor);
	}

	@Override
	protected void logStart()
	{
		log.debug("Counting distinct objects for {} and predicate {}", cv.gd().getGraphName(), predicatePartition.getPredicate());
	}

	@Override
	protected void logEnd()
	{
		log.debug("Counted distinct {} objects for {} and predicate {}", predicatePartition.getDistinctObjectCount(),
				cv.gd().getGraphName(), predicatePartition.getPredicate());
	}

	@Override
	protected Long run(RepositoryConnection connection) 
		throws QueryEvaluationException, RepositoryException, MalformedQueryException
	{
		Resource predicate = predicatePartition.getPredicate();
		MapBindingSet bindingSet = new MapBindingSet();
		bindingSet.setBinding("graph", cv.gd().getGraph());
		bindingSet.setBinding("predicate", predicate);
		setQuery(rawQuery, bindingSet);
		return Helper.getSingleLongFromSparql(getQuery(), connection, OBJECTS);
	}

	@Override
	protected void set(Long subjects)
	{
		try
		{
			cv.writeLock().lock();
			predicatePartition.setDistinctObjectCount(subjects);
		} finally {
			cv.writeLock().unlock();
		}
	}
	
	@Override
	protected Logger getLog() {
		return log;
	}
}