package swiss.sib.swissprot.voidcounter.virtuoso;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;
import virtuoso.rdf4j.driver.VirtuosoRepository;
import virtuoso.rdf4j.driver.VirtuosoRepositoryConnection;

public class CountUniqueObjectsPerPredicateInGraph
    extends QueryCallable<Long>
{
	private static final Logger log = LoggerFactory.getLogger(CountUniqueObjectsPerPredicateInGraph.class);

	private final PredicatePartition predicatePartition;
	private final CommonVariables cv;
	
	public CountUniqueObjectsPerPredicateInGraph(CommonVariables cv, PredicatePartition predicatePartition)
	{
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		assert cv.repository() instanceof VirtuosoRepository;
		this.cv = cv;
		this.predicatePartition = predicatePartition;
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
		if (connection instanceof VirtuosoRepositoryConnection)
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
			throw new IllegalStateException("Not a Virtuoso connection");
		}
	}

	private long countDistinctLiteralObjects(RepositoryConnection connection, Resource predicate, long countOfLiterals)
	    throws MalformedQueryException, RepositoryException
	{
		setQuery("SELECT (COUNT(DISTINCT(RDF_QUAD.O))) FROM RDF_QUAD WHERE RDF_QUAD.G = iri_to_id('"
		    + cv.gd().getGraphName() + "') AND RDF_QUAD.P = iri_to_id('" + predicate
		    + "') AND isiri_id(RDF_QUAD.O) = 0");
		VirtuosoRepositoryConnection virtuosoRepositoryConnection = (VirtuosoRepositoryConnection) connection;
		Connection vrc = virtuosoRepositoryConnection.getQuadStoreConnection();
		try (java.sql.Statement stat = vrc.createStatement())
		{
			try (ResultSet res = stat.executeQuery(getQuery()))
			{
				log.debug("Counting literal objects for {} and predicate {}", cv.gd().getGraphName(), predicate);
				while (res.next())
				{
					countOfLiterals = res.getLong(1);
				}
				log.debug("Counted {} literal objects for {} and predicate {}", countOfLiterals, cv.gd().getGraphName(), predicate);
			} catch (SQLException e)
			{
				log.debug("Failed to query error:" + e.getMessage());
				throw new MalformedQueryException(e);
			}
		} catch (SQLException e)
		{
			log.debug("Failed to count" + e.getMessage() + " literal objects for " + cv.gd().getGraphName()
			    + " and predicate " + predicate);
			throw new RepositoryException(e);
		}
		return countOfLiterals;
	}

	private long countDistinctIriObjects(RepositoryConnection connection, Resource predicate)
	    throws QueryEvaluationException, RepositoryException
	{
		//See http://docs.openlinksw.com/virtuoso/rdfiriidtype/
		setQuery("SELECT iri_id_num(RDF_QUAD.O) FROM RDF_QUAD WHERE RDF_QUAD.G = iri_to_id('"
		    + cv.gd().getGraphName() + "') AND RDF_QUAD.P = iri_to_id('" + predicate
		    + "') AND isiri_id(RDF_QUAD.O) = 1");
		return VirtuosoFromSQL.countDistinctLongResultsFromVirtuoso(connection, getQuery());
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