package swiss.sib.swissprot.voidcounter;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.LinkSetToOtherGraph;
import swiss.sib.swissprot.servicedescription.sparql.Helper;

public final class CountTriplesLinkingTwoTypesInDifferentGraphs extends QueryCallable<Long> {
	private static final Logger log = LoggerFactory.getLogger(CountTriplesLinkingTwoTypesInDifferentGraphs.class);
	private final Lock writeLock;
	private final AtomicInteger finishedQueries;
	private final LinkSetToOtherGraph ls;
	private final GraphDescription gd;

	public CountTriplesLinkingTwoTypesInDifferentGraphs(GraphDescription gd, LinkSetToOtherGraph ls,
			Repository repository, Lock writeLock, Semaphore limiter, AtomicInteger scheduledQueries,
			AtomicInteger finishedQueries) {
		super(repository, limiter);
		this.gd = gd;
		this.ls = ls;
		this.writeLock = writeLock;
		this.finishedQueries = finishedQueries;
		scheduledQueries.incrementAndGet();
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
		String q = "SELECT (COUNT(?target) AS ?lsc) WHERE  { GRAPH <" + gd.getGraphName() + ">{ ?subject a <"
				+ sourceType + "> } ?subject <" + predicate + "> ?target . GRAPH <" + otherGraphName + "> {?target a <"
				+ targetType + "> }}";
		try {
			return Helper.getSingleLongFromSparql(q, connection, "lsc");
		} finally {
			finishedQueries.incrementAndGet();
		}
	}

	@Override
	protected void set(Long count) {
		try {
			writeLock.lock();
			ls.setTripleCount(count);
		} finally {
			writeLock.unlock();
		}

	}
}