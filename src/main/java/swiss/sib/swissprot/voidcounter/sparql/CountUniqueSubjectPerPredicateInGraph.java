package swiss.sib.swissprot.voidcounter.sparql;

import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

class CountUniqueSubjectPerPredicateInGraph
    extends QueryCallable<Long>
{

	private static final String SUBJECTS = "subjects";
	private final String rawQuery;
	public static final Logger log = LoggerFactory.getLogger(CountUniqueSubjectPerPredicateInGraph.class);

	private final PredicatePartition predicatePartition;
	
	public CountUniqueSubjectPerPredicateInGraph(CommonVariables cv, PredicatePartition predicatePartition, OptimizeFor optimizeFor)
	{
		super(cv);
		this.predicatePartition = predicatePartition;
		this.rawQuery =  Helper.loadSparqlQuery("count_distinct_subjects_for_a_predicate_in_a_graph", optimizeFor);
	}

	@Override
	protected void logStart()
	{
		log.debug("Counting distinct subjects in {} for predicate {}", cv.gd().getGraphName(), predicatePartition.getPredicate());

	}

	@Override
	protected void logEnd()
	{
		log.debug("Counted distinct subjects in {} for predicate {}", cv.gd().getGraphName(), predicatePartition.getPredicate());
	}

	@Override
	protected Long run(RepositoryConnection connection)
	    throws Exception
	{
		MapBindingSet bs = new MapBindingSet();
		bs.setBinding("graph", cv.gd().getGraph() );
		bs.setBinding("predicate", predicatePartition.getPredicate());
		setQuery(rawQuery, bs);
		return Helper.getSingleLongFromSparql(getQuery(), connection, SUBJECTS);
	}

	@Override
	protected void set(Long subjects)
	{
		try	{
			cv.writeLock().lock();
			predicatePartition.setDistinctSubjectCount(subjects);
		} finally {
			cv.writeLock().unlock();
		}
	}
	
	@Override
	protected Logger getLog() {
		return log;
	}
}