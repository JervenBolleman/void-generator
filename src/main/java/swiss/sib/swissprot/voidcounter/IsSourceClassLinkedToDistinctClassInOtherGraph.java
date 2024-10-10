package swiss.sib.swissprot.voidcounter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

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
import swiss.sib.swissprot.servicedescription.LinkSetToOtherGraph;
import swiss.sib.swissprot.servicedescription.PredicatePartition;

public final class IsSourceClassLinkedToDistinctClassInOtherGraph extends QueryCallable<List<LinkSetToOtherGraph>> {
	private static final String query = """
			SELECT DISTINCT ?targetType
			WHERE {
				GRAPH ?sourceGraphName { ?subject a ?sourceType . }
				?subject ?predicate ?target .
				GRAPH ?targetGraphName {?target a ?targetType }
			}
			""";

	public static final Logger log = LoggerFactory.getLogger(IsSourceClassLinkedToDistinctClassInOtherGraph.class);

	private final IRI predicate;
	private final PredicatePartition predicatePartition;
	private final ClassPartition source;
	private final GraphDescription gd;
	private final Lock writeLock;
	private final AtomicInteger finishedQueries;
	private final GraphDescription otherGraph;


	private final Function<QueryCallable<?>, CompletableFuture<Exception>> schedule;

	private final String classExclusion;


	public IsSourceClassLinkedToDistinctClassInOtherGraph(Repository repository, IRI predicate,
			PredicatePartition predicatePartition, ClassPartition source, GraphDescription gd, Lock writeLock,
			Semaphore limiter, AtomicInteger finishedQueries, GraphDescription otherGraph, Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, String classExclusion) {
		super(repository, limiter);
		this.predicate = predicate;
		this.predicatePartition = predicatePartition;
		this.source = source;
		this.gd = gd;
		this.writeLock = writeLock;
		this.finishedQueries = finishedQueries;
		this.otherGraph = otherGraph;
		this.schedule = schedule;
		this.classExclusion = classExclusion;
	}

	@Override
	protected void logStart() {
		log.debug("Checking if " + source.getClazz() + " in " + gd.getGraphName() + " connected to "
				+ otherGraph.getGraphName() + " via " + predicate);
	}

	@Override
	protected void logEnd() {
		log.debug("Checked if " + source.getClazz() + " in " + gd.getGraphName() + " connected to "
				+ otherGraph.getGraphName() + " via " + predicate);
	}

	@Override
	protected List<LinkSetToOtherGraph> run(RepositoryConnection connection) throws Exception {
		try {
			final IRI sourceType = source.getClazz();

			final TupleQuery pbq = connection.prepareTupleQuery(QueryLanguage.SPARQL, makeQuery());
			pbq.setBinding("sourceGraphName", gd.getGraph());
			pbq.setBinding("predicate", predicate);
			pbq.setBinding("targetGraphName", otherGraph.getGraph());
			pbq.setBinding("sourceType", sourceType);
			setQuery(query, pbq.getBindings());
			List<LinkSetToOtherGraph> res = new ArrayList<>();
			try (TupleQueryResult tqr = pbq.evaluate()) {
				while (tqr.hasNext()) {
					IRI targetType = (IRI) tqr.next().getBinding("targetType").getValue();
					LinkSetToOtherGraph subTarget = new LinkSetToOtherGraph(predicatePartition, targetType, sourceType,
							otherGraph);
					res.add(subTarget);
					schedule.apply(new CountTriplesLinkingTwoTypesInDifferentGraphs(gd, subTarget, repository, writeLock, limiter, finishedQueries));
				}
			}
			return res;
		} finally {
			finishedQueries.incrementAndGet();
		}
	}

	private String makeQuery() {
		if (classExclusion == null) {
			return query;
		} else {
			return "";
		}
	}

	@Override
	protected void set(List<LinkSetToOtherGraph> has) {
		if (!has.isEmpty()) {
			try {
				writeLock.lock();
				for (LinkSetToOtherGraph subTarget : has) {
					predicatePartition.putLinkPartition(subTarget);
				}
			} finally {
				writeLock.unlock();
			}
		}

	}
}
