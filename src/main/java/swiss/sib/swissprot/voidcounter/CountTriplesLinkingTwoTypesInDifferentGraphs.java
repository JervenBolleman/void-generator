package swiss.sib.swissprot.voidcounter;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.LinkSetToOtherGraph;
import swiss.sib.swissprot.servicedescription.PredicatePartition;

public final class CountTriplesLinkingTwoTypesInDifferentGraphs extends QueryCallable<Long> {
	private static final Logger log = LoggerFactory.getLogger(CountTriplesLinkingTwoTypesInDifferentGraphs.class);
	
	private static final String COUNT_TRIPLES_LINKING = """
			SELECT (COUNT(?target) AS ?lsc) 
			WHERE  { 
				GRAPH ?graphName { 
					?subject a ?sourceType 
				} 
				GRAPH ?linkingGraphName { 
					?subject ?predicate ?target
				} 
				GRAPH ?otherGraphName {
				 	?target a ?targetType 
				}
			}
			""";
	private final Lock writeLock;
	private final LinkSetToOtherGraph ls;
	private final GraphDescription gd;

	private final PredicatePartition predicatePartition;


	public CountTriplesLinkingTwoTypesInDifferentGraphs(GraphDescription gd, LinkSetToOtherGraph ls,
			Repository repository, Lock writeLock, Semaphore limiter,
			AtomicInteger finishedQueries, PredicatePartition predicatePartition) {
		super(repository, limiter, finishedQueries);
		this.gd = gd;
		this.ls = ls;
		this.writeLock = writeLock;
		this.predicatePartition = predicatePartition;
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct triples between for " + ls);
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("failed counting triples between classses " + ls, e);
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct triples:" + ls.getTripleCount() + " for " + ls);
	}

	@Override
	protected Long run(RepositoryConnection connection)
			throws MalformedQueryException, QueryEvaluationException, RepositoryException {
		IRI predicate = ls.getPredicatePartition().getPredicate();
		String otherGraphName = ls.getOtherGraph().getGraphName();
		IRI targetType = ls.getTargetType();
		IRI sourceType = ls.getSourceType();
		assert targetType != null;
		assert sourceType != null;
		assert otherGraphName != null;
		assert predicate != null;
		
		
		TupleQuery tupleQuery = connection.prepareTupleQuery(COUNT_TRIPLES_LINKING);

		SimpleValueFactory vf = SimpleValueFactory.getInstance();
		tupleQuery.setBinding("graphName", vf.createIRI(gd.getGraphName()));
		tupleQuery.setBinding("sourceType", sourceType);
		tupleQuery.setBinding("predicate", predicate);
		tupleQuery.setBinding("otherGraphName", vf.createIRI(otherGraphName));
		tupleQuery.setBinding("targetType", targetType);
		tupleQuery.setBinding("linkingGraphName", ls.getLinkingGraph());
		setQuery(COUNT_TRIPLES_LINKING, tupleQuery.getBindings());
		
		BindingSet next = tupleQuery.evaluate().next();
		return ((Literal) next.getBinding("lsc").getValue()).longValue();
	}

	@Override
	protected void set(Long count) {
		try {
			writeLock.lock();
			if (count > 0) {
				predicatePartition.putLinkPartition(ls);
				ls.setTripleCount(count);
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