package swiss.sib.swissprot.voidcounter.virtuoso;

import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;
import virtuoso.rdf4j.driver.VirtuosoRepositoryConnection;

class CountUniqueSubjectPerPredicateInGraph
    extends QueryCallable<Long>
{

	private static final String SUBJECTS = "subjects";
	private static final String QUERY = Helper.loadSparqlQuery("count_distinct_subjects_for_a_predicate_in_a_graph");
	private static final Logger log = LoggerFactory.getLogger(CountUniqueSubjectPerPredicateInGraph.class);

	private final CommonVariables cv;
	private final PredicatePartition predicatePartition;
	
	public CountUniqueSubjectPerPredicateInGraph(CommonVariables cv, PredicatePartition predicatePartition)
	{
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;
		this.predicatePartition = predicatePartition;
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
		if (connection instanceof VirtuosoRepositoryConnection
		    && predicatePartition.getTripleCount() > SWITCH_TO_OPTIMIZED_COUNT_AT)
		{
			//See http://docs.openlinksw.com/virtuoso/rdfiriidtype/
			setQuery("SELECT iri_id_num(RDF_QUAD.S) FROM RDF_QUAD WHERE RDF_QUAD.G = iri_to_id('"
			    + cv.gd().getGraphName() + "') AND RDF_QUAD.P = iri_to_id('" + predicatePartition.getPredicate() + "')");
			return VirtuosoFromSQL.countDistinctLongResultsFromVirtuoso(connection, getQuery());
		}
		else
		{
			MapBindingSet bs = new MapBindingSet();
			bs.setBinding("graph", cv.gd().getGraph() );
			bs.setBinding("predicate", predicatePartition.getPredicate());
			setQuery(QUERY, bs);
			return Helper.getSingleLongFromSparql(getQuery(), connection, SUBJECTS);
		}
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