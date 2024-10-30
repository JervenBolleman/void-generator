package swiss.sib.swissprot.voidcounter;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.BindingSet;
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
	private static final String QUERY = """
			SELECT ?clazz ?linkingGraphName (COUNT(?subject) AS ?count)
			WHERE {
				GRAPH ?sourceGraphName {
					?subject a ?sourceType
					?subject ?predicate ?target
				}
				GRAPH ?targetGraphName {
					?target a ?clazz
				}
			} GROUP BY ?clazz ?linkingGraphName
			""";

	public static final Logger log = LoggerFactory.getLogger(IsSourceClassLinkedToDistinctClassInOtherGraph.class);

	private final IRI predicate;
	private final PredicatePartition predicatePartition;
	private final ClassPartition source;
	private final GraphDescription gd;
	private final Lock writeLock;
	private final GraphDescription otherGraph;

	private final String classExclusion;

	private final Function<QueryCallable<?>, CompletableFuture<Exception>> scheduler;

	//TODO pass in a different waiter/limiter and logic to make sure that the other graph class list is known before we start here.
	//this can't use the current limiter logic as that would deadlock.
	public IsSourceClassLinkedToDistinctClassInOtherGraph(Repository repository, IRI predicate,
			PredicatePartition predicatePartition, ClassPartition source, GraphDescription gd, Lock writeLock,
			Semaphore limiter, AtomicInteger finishedQueries, GraphDescription otherGraph,
			Function<QueryCallable<?>, CompletableFuture<Exception>> scheduler, String classExclusion) {
		super(repository, limiter, finishedQueries);
		this.predicate = predicate;
		this.predicatePartition = predicatePartition;
		this.source = source;
		this.gd = gd;
		this.writeLock = writeLock;
		this.otherGraph = otherGraph;
		this.scheduler = scheduler;
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
		final IRI sourceType = source.getClazz();

		if (otherGraph.getPredicates().isEmpty()) {
			return rediscoverPossibleLinkClasses(connection, sourceType);
		} else {
			for (ClassPartition cp : otherGraph.getClasses()) {
				LinkSetToOtherGraph ls = new LinkSetToOtherGraph(predicatePartition, sourceType, cp.getClazz(), otherGraph, gd.getGraph());
				scheduler.apply(new CountTriplesLinkingTwoTypesInDifferentGraphs(gd, ls, repository, writeLock, limiter, finishedQueries, predicatePartition));
			}
			return List.of();    
		}
	}

	public List<LinkSetToOtherGraph> rediscoverPossibleLinkClasses(RepositoryConnection connection,
			final IRI sourceType) throws Exception {
		String rq = makeQuery(QUERY, otherGraph, classExclusion);
		final TupleQuery pbq = connection.prepareTupleQuery(QueryLanguage.SPARQL, rq);
		pbq.setBinding("sourceGraphName", gd.getGraph());
		pbq.setBinding("predicate", predicate);
		pbq.setBinding("targetGraphName", otherGraph.getGraph());
		pbq.setBinding("sourceType", sourceType);
		setQuery(rq, pbq.getBindings());
		List<LinkSetToOtherGraph> pps = new ArrayList<>();
		try (TupleQueryResult tqr = pbq.evaluate()) {
			while (tqr.hasNext()) {
				BindingSet next = tqr.next();
				long count = ((Literal) next.getBinding("count").getValue()).longValue();
				if (count > 0) {
					IRI targetType = (IRI) next.getBinding("clazz").getValue();
					LinkSetToOtherGraph subTarget = new LinkSetToOtherGraph(predicatePartition, targetType, sourceType,
							otherGraph, gd.getGraph());
					subTarget.setTripleCount(count);
					pps.add(subTarget);
				}
			}
		}
		return pps;
	}

	private static String makeQuery(String query, GraphDescription otherGraph, String classExclusion) {
		Set<ClassPartition> classes = otherGraph.getClasses();
		String withKnownClasses = addKnownClasses(query, classes);
		//If we already know the other classes then we do not need to filter them out
		if (classExclusion == null || !classes.isEmpty()) {
			return withKnownClasses;
		} else {
			return insertBeforeEnd(withKnownClasses, "\tFILTER(" + classExclusion + ")\n");
		}
	}

	private static String addKnownClasses(String query2, Set<ClassPartition> classes) {
		if (classes.isEmpty())
			return query2;
		else {
			String collect = classes.stream().map(ClassPartition::getClazz).map(c -> "(<" + c.stringValue() + ">)")
					.collect(Collectors.joining(" "));
			return insertBeforeEnd(query2, "\tVALUES (?clazz) {" + collect + "}\n");
		}
	}

	private static String insertBeforeEnd(String query, String collect) {
		return new StringBuilder(query).insert(query.lastIndexOf('}') - 1, collect).toString();
	}

	@Override
	protected void set(List<LinkSetToOtherGraph> pps) {
		try {
			writeLock.lock();
			for (LinkSetToOtherGraph lstog : pps) {
				predicatePartition.putLinkPartition(lstog);
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
