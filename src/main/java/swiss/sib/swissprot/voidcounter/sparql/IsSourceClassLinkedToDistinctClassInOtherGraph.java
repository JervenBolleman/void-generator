package swiss.sib.swissprot.voidcounter.sparql;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.LinkSetToOtherGraph;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

public final class IsSourceClassLinkedToDistinctClassInOtherGraph extends QueryCallable<List<LinkSetToOtherGraph>> {
	private static final String QUERY = Helper.loadSparqlQuery("inter_graph_links_grouped_by_type");

	public static final Logger log = LoggerFactory.getLogger(IsSourceClassLinkedToDistinctClassInOtherGraph.class);

	private final IRI predicate;
	private final PredicatePartition pp;
	private final ClassPartition source;
	private final String classExclusion;

	private final Function<QueryCallable<?>, CompletableFuture<Exception>> scheduler;

	private final GraphDescription otherGraph;

	private final CommonVariables cv;


	//TODO pass in a different waiter/limiter and logic to make sure that the other graph class list is known before we start here.
	//this can't use the current limiter logic as that would deadlock.
	public IsSourceClassLinkedToDistinctClassInOtherGraph(CommonVariables cv,
			PredicatePartition predicatePartition, ClassPartition source,
			GraphDescription otherGraph,
			Function<QueryCallable<?>, CompletableFuture<Exception>> scheduler, String classExclusion) {
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;
		this.predicate = predicatePartition.getPredicate();
		this.pp = predicatePartition;
		this.source = source;
		this.otherGraph = otherGraph;
		this.scheduler = scheduler;
		this.classExclusion = classExclusion;
	}

	@Override
	protected void logStart() {
		log.debug("Checking if {} in {} connected to {} via {}", source.getClazz(), cv.gd().getGraphName(), otherGraph.getGraphName(), pp.getPredicate());
	}

	@Override
	protected void logEnd() {
		log.debug("Checked if {} in {} connected to {} via {}", source.getClazz(), cv.gd().getGraphName(), otherGraph.getGraphName(), pp.getPredicate());
	}

	@Override
	protected List<LinkSetToOtherGraph> run(RepositoryConnection connection) throws Exception {
		final IRI sourceType = source.getClazz();

		if (otherGraph.getPredicates().isEmpty()) {
			return rediscoverPossibleLinkClasses(connection, sourceType);
		} else {
			for (ClassPartition cp : otherGraph.getClasses()) {
				LinkSetToOtherGraph ls = new LinkSetToOtherGraph(pp, sourceType, cp.getClazz(), otherGraph, cv.gd().getGraph());
				scheduler.apply(new CountTriplesLinkingTwoTypesInDifferentGraphs(cv, ls, pp));
			}
			return List.of();    
		}
	}

	public List<LinkSetToOtherGraph> rediscoverPossibleLinkClasses(RepositoryConnection connection,
			final IRI sourceType) {
		String rq = makeQuery(QUERY, otherGraph, classExclusion);
		final MapBindingSet pbq = new MapBindingSet();
		pbq.setBinding("sourceGraphName", cv.gd().getGraph());
		pbq.setBinding("predicate", predicate);
		pbq.setBinding("targetGraphName", otherGraph.getGraph());
		pbq.setBinding("sourceType", sourceType);
		setQuery(rq, pbq);
		List<LinkSetToOtherGraph> pps = new ArrayList<>();
		try (TupleQueryResult tqr = Helper.runTupleQuery(getQuery(), connection)) {
			while (tqr.hasNext()) {
				BindingSet next = tqr.next();
				long count = ((Literal) next.getBinding("count").getValue()).longValue();
				if (count > 0) {
					IRI targetType = (IRI) next.getBinding("clazz").getValue();
					LinkSetToOtherGraph subTarget = new LinkSetToOtherGraph(pp, targetType, sourceType,
							otherGraph, cv.gd().getGraph());
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
			cv.writeLock().lock();
			for (LinkSetToOtherGraph lstog : pps) {
				pp.putLinkPartition(lstog);
			}
		} finally {
			cv.writeLock().unlock();
		}
	}
	
	@Override
	protected Logger getLog() {
		return log;
	}
}
