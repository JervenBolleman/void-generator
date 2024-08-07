package swiss.sib.swissprot.voidcounter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.PredicatePartition;

public final class IsSourceClassLinkedToDistinctClassInOtherGraph extends QueryCallable<List<IRI>> {

	public static final Logger log = LoggerFactory.getLogger(IsSourceClassLinkedToDistinctClassInOtherGraph.class);

	private final IRI predicate;
	private final PredicatePartition predicatePartition;
	private final ClassPartition source;
	private final GraphDescription gd;
	private final Lock writeLock;
	private final AtomicInteger finishedQueries;
	private final GraphDescription otherGraph;

	public IsSourceClassLinkedToDistinctClassInOtherGraph(Repository repository, IRI predicate,
			PredicatePartition predicatePartition, ClassPartition source, GraphDescription gd, Lock writeLock,
			Semaphore limiter, AtomicInteger scheduledQueries, AtomicInteger finishedQueries, GraphDescription otherGraph) {
		super(repository, limiter);
		this.predicate = predicate;
		this.predicatePartition = predicatePartition;
		this.source = source;
		this.gd = gd;
		this.writeLock = writeLock;
		this.finishedQueries = finishedQueries;
		this.otherGraph = otherGraph;
		scheduledQueries.incrementAndGet();
	}

	@Override
	protected void logStart() {
		log.debug("Checking if " + source.getClazz() + " in " + gd.getGraphName() +" connected to " + otherGraph.getGraphName() + " via " + predicate);
	}

	@Override
	protected void logEnd() {
		log.debug("Checked if " + source.getClazz() + " in " + gd.getGraphName() +" connected to " + otherGraph.getGraphName() + " via " + predicate);
	}

	@Override
	protected List<IRI> run(RepositoryConnection connection) throws Exception {
		try {
			final IRI sourceType = source.getClazz();		
			final String query = "SELECT DISTINCT ?targetType WHERE  { GRAPH <" + gd.getGraphName() + ">{ ?subject a <" + sourceType + "> . } ?subject <"
					+ predicate + "> ?target . GRAPH <"+otherGraph.getGraphName()+"> {?target a ?targetType }}";
			final TupleQuery pbq = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
			List<IRI> res = new ArrayList<>();
			try (TupleQueryResult tqr=pbq.evaluate()){
				while(tqr.hasNext()) {
					res.add((IRI) tqr.next().getBinding("targetType").getValue());
				}
			}
			return res;
		} finally {
			finishedQueries.incrementAndGet();
		}
	}

	@Override
	protected void set(List<IRI> has) {
		if (!has.isEmpty()) {
			try {
				writeLock.lock();
				for (IRI targetType:has) {
					LinkSet subTarget = new LinkSet(predicatePartition, targetType, otherGraph);
					predicatePartition.putLinkPartition(subTarget);
				}
			} finally {
				writeLock.unlock();
			}
		}

	}
}