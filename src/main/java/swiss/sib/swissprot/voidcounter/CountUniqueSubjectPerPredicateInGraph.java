package swiss.sib.swissprot.voidcounter;

import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.virtuoso.VirtuosoFromSQL;
import virtuoso.rdf4j.driver.VirtuosoRepositoryConnection;

public class CountUniqueSubjectPerPredicateInGraph
    extends QueryCallable<Long>
{

	private final PredicatePartition predicatePartition;
	private final GraphDescription gd;
	public static final Logger log = LoggerFactory.getLogger(CountUniqueSubjectPerPredicateInGraph.class);
	private final Lock writeLock;
	
	public CountUniqueSubjectPerPredicateInGraph(GraphDescription gd, PredicatePartition predicatePartition,
	    Repository repository, Lock writeLock, Semaphore limiter)
	{
		super(repository, limiter);
		this.gd = gd;
		this.predicatePartition = predicatePartition;
		this.writeLock = writeLock;
	}

	@Override
	protected void logStart()
	{
		log.debug("Counting distinct subjects for " + gd.getGraphName() + " and predicate "
		    + predicatePartition.getPredicate());

	}

	@Override
	protected void logEnd()
	{
		log.debug(
		    "Counted distinct " + predicatePartition.getDistinctSubjectCount() + " subjects for "
		        + gd.getGraphName() + " and predicate " + predicatePartition.getPredicate());
	}

	@Override
	protected Long run(RepositoryConnection connection)
	    throws Exception
	{
		if (connection instanceof VirtuosoRepositoryConnection
		    && predicatePartition.getTripleCount() > SWITCH_TO_OPTIMIZED_COUNT_AT)
		{
			//See http://docs.openlinksw.com/virtuoso/rdfiriidtype/
			String sql = "SELECT iri_id_num(RDF_QUAD.S) FROM RDF_QUAD WHERE RDF_QUAD.G = iri_to_id('"
			    + gd.getGraphName() + "') AND RDF_QUAD.P = iri_to_id('" + predicatePartition.getPredicate() + "')";
			return VirtuosoFromSQL.countDistinctLongResultsFromVirtuoso(connection, sql);
		}
		else
		{
			final String countDistinctSubjectQuery = "SELECT (count(distinct(?subject)) AS ?types) WHERE { GRAPH <"
			    + gd.getGraphName() + "> WHERE {?subject <" + predicatePartition.getPredicate() + "> ?object}}";
			return ((Literal) Helper.getFirstNumberResultFromTupleQuery(countDistinctSubjectQuery, connection))
			    .longValue();
		}
	}

	@Override
	protected void set(Long subjects)
	{
		try	{
			writeLock.lock();
			predicatePartition.setDistinctSubjectCount(subjects);
		} finally {
			writeLock.unlock();
		}
	}
}