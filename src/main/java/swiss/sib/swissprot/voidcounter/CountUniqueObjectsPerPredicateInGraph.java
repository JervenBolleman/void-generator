package swiss.sib.swissprot.voidcounter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.virtuoso.VirtuosoFromSQL;
import virtuoso.rdf4j.driver.VirtuosoRepositoryConnection;

public class CountUniqueObjectsPerPredicateInGraph
    extends QueryCallable<Long>
{

	private static final String OBJECTS = "objects";
	private final PredicatePartition predicatePartition;
	private final GraphDescription gd;
	private static final Logger log = LoggerFactory.getLogger(CountUniqueObjectsPerPredicateInGraph.class);
	private final Lock writeLock;
	
	public CountUniqueObjectsPerPredicateInGraph(GraphDescription gd, PredicatePartition predicatePartition,
	    Repository repository, Lock writeLock, Semaphore limiter, AtomicInteger finishedQueries)
	{
		super(repository, limiter, finishedQueries);
		this.gd = gd;
		this.predicatePartition = predicatePartition;
		this.writeLock = writeLock;
	}

	@Override
	protected void logStart()
	{
		log.debug("Counting distinct objects for " + gd.getGraphName() + " and predicate "
		    + predicatePartition.getPredicate());

	}

	@Override
	protected void logEnd()
	{
		log.debug(
		    "Counted distinct " + predicatePartition.getDistinctObjectCount() + " objects for "
		        + gd.getGraphName() + " and predicate " + predicatePartition.getPredicate());
	}

	@Override
	protected Long run(RepositoryConnection connection) 
		throws QueryEvaluationException, RepositoryException, MalformedQueryException
	{
		Resource predicate = predicatePartition.getPredicate();
		if (connection instanceof VirtuosoRepositoryConnection
		    && predicatePartition.getTripleCount() > SWITCH_TO_OPTIMIZED_COUNT_AT)
		{
			long countDistinctIri = countDistinctIriObjects(connection, predicate);
			long countOfLiterals = 0;
			//If the number of distinctIri equals the number of triples there won't be any literals.
			if (countDistinctIri != predicatePartition.getTripleCount())
			{
				countOfLiterals = countDistinctLiteralObjects(connection, predicate, countOfLiterals);
			}
			return countDistinctIri + countOfLiterals;
		}
		else
		{
			query = "SELECT (count(distinct ?object) as ?objects) WHERE { GRAPH <"
			    + gd.getGraphName() + "> {?subject <" + predicate + "> ?object}}";
			return Helper.getSingleLongFromSparql(query, connection, OBJECTS);
		}
	}

	private long countDistinctLiteralObjects(RepositoryConnection connection, Resource predicate, long countOfLiterals)
	    throws MalformedQueryException, RepositoryException
	{
		query = "SELECT (COUNT(DISTINCT(RDF_QUAD.O))) FROM RDF_QUAD WHERE RDF_QUAD.G = iri_to_id('"
		    + gd.getGraphName() + "') AND RDF_QUAD.P = iri_to_id('" + predicate
		    + "') AND isiri_id(RDF_QUAD.O) = 0";
		VirtuosoRepositoryConnection virtuosoRepositoryConnection = (VirtuosoRepositoryConnection) connection;
		Connection vrc = virtuosoRepositoryConnection.getQuadStoreConnection();
		try (java.sql.Statement stat = vrc.createStatement())
		{
			try (ResultSet res = stat.executeQuery(query))
			{
				log.debug(
				    "Counting literal objects for " + gd.getGraphName() + " and predicate " + predicate);
				while (res.next())
				{
					countOfLiterals = res.getLong(1);
				}
				log.debug("Counted " + countOfLiterals + " literal objects for " + gd.getGraphName()
				    + " and predicate " + predicate);
			} catch (SQLException e)
			{
				log.debug("Failed to query error:" + e.getMessage());
				throw new MalformedQueryException(e);
			}
		} catch (SQLException e)
		{
			log.debug("Failed to count" + e.getMessage() + " literal objects for " + gd.getGraphName()
			    + " and predicate " + predicate);
			throw new RepositoryException(e);
		}
		return countOfLiterals;
	}

	private long countDistinctIriObjects(RepositoryConnection connection, Resource predicate)
	    throws QueryEvaluationException, RepositoryException
	{
		//See http://docs.openlinksw.com/virtuoso/rdfiriidtype/
		String sql = "SELECT iri_id_num(RDF_QUAD.O) FROM RDF_QUAD WHERE RDF_QUAD.G = iri_to_id('"
		    + gd.getGraphName() + "') AND RDF_QUAD.P = iri_to_id('" + predicate
		    + "') AND isiri_id(RDF_QUAD.O) = 1";
		return VirtuosoFromSQL.countDistinctLongResultsFromVirtuoso(connection, sql);
	}

	@Override
	protected void set(Long subjects)
	{
		try
		{
			writeLock.lock();
			predicatePartition.setDistinctObjectCount(subjects);
		} finally {
			writeLock.unlock();
		}
	}
}