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
			SELECT DISTINCT ?clazz
			WHERE {
				GRAPH ?sourceGraphName { ?subject a ?sourceType . }
				?subject ?predicate ?target .
				GRAPH ?targetGraphName {?target a ?clazz }
			}
			""";

	public static final Logger log = LoggerFactory.getLogger(IsSourceClassLinkedToDistinctClassInOtherGraph.class);

	private final IRI predicate;
	private final PredicatePartition predicatePartition;
	private final ClassPartition source;
	private final GraphDescription gd;
	private final Lock writeLock;
	private final GraphDescription otherGraph;

	private final Function<QueryCallable<?>, CompletableFuture<Exception>> schedule;

	private final String classExclusion;

	public IsSourceClassLinkedToDistinctClassInOtherGraph(Repository repository, IRI predicate,
			PredicatePartition predicatePartition, ClassPartition source, GraphDescription gd, Lock writeLock,
			Semaphore limiter, AtomicInteger finishedQueries, GraphDescription otherGraph,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, String classExclusion) {
		super(repository, limiter, finishedQueries);
		this.predicate = predicate;
		this.predicatePartition = predicatePartition;
		this.source = source;
		this.gd = gd;
		this.writeLock = writeLock;
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
		final IRI sourceType = source.getClazz();

		String rq = makeQuery(QUERY, otherGraph, classExclusion);
		final TupleQuery pbq = connection.prepareTupleQuery(QueryLanguage.SPARQL, rq);
		pbq.setBinding("sourceGraphName", gd.getGraph());
		pbq.setBinding("predicate", predicate);
		pbq.setBinding("targetGraphName", otherGraph.getGraph());
		pbq.setBinding("sourceType", sourceType);
		setQuery(rq, pbq.getBindings());
		List<LinkSetToOtherGraph> res = new ArrayList<>();
		try (TupleQueryResult tqr = pbq.evaluate()) {
			while (tqr.hasNext()) {
				IRI targetType = (IRI) tqr.next().getBinding("clazz").getValue();
				LinkSetToOtherGraph subTarget = new LinkSetToOtherGraph(predicatePartition, targetType, sourceType,
						otherGraph);
				res.add(subTarget);
				schedule.apply(new CountTriplesLinkingTwoTypesInDifferentGraphs(gd, subTarget, repository, writeLock,
						limiter, finishedQueries));
			}
		}
		return res;
	}

	private static String makeQuery(String query, GraphDescription otherGraph, String classExclusion) {
		String withKnownClasses = addKnownClasses(query, otherGraph.getClasses());
		if (classExclusion == null) {
			return withKnownClasses;
		} else {
			return insertBeforeEnd(withKnownClasses, "   FILTER(" + classExclusion + ")\n");
		}
	}

	private static String addKnownClasses(String query2, Set<ClassPartition> classes) {
		if (classes.isEmpty())
			return query2;
		else {
			String collect = classes.stream().map(ClassPartition::getClazz).map(c -> "(<" + c.stringValue() + ">)")
					.collect(Collectors.joining(" "));
			return insertBeforeEnd(query2, "VALUES (?clazz) {"+collect+"}\n");
		}
	}

	private static String insertBeforeEnd(String query, String collect) {
		return new StringBuilder(query).insert(query.length() - 2, collect).toString();
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
