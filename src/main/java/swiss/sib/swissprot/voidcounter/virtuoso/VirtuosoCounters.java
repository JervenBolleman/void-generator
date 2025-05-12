package swiss.sib.swissprot.voidcounter.virtuoso;

import static swiss.sib.swissprot.servicedescription.OptimizeFor.VIRTUOSO;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;

import org.eclipse.rdf4j.model.IRI;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.FindGraphs;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.LinkSetToOtherGraph;
import swiss.sib.swissprot.servicedescription.ObjectPartition;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.Counters;
import swiss.sib.swissprot.voidcounter.QueryCallable;
import swiss.sib.swissprot.voidcounter.sparql.CountTriplesLinkingTwoTypesInDifferentGraphs;
import swiss.sib.swissprot.voidcounter.sparql.FindDistinctClassses;
import swiss.sib.swissprot.voidcounter.sparql.FindNamedIndividualObjectSubjectForPredicateInGraph;
import swiss.sib.swissprot.voidcounter.sparql.FindPredicateLinkSets;
import swiss.sib.swissprot.voidcounter.sparql.FindPredicatesAndClasses;
import swiss.sib.swissprot.voidcounter.sparql.FindPredicatesAndCountObjects;
import swiss.sib.swissprot.voidcounter.sparql.IsSourceClassLinkedToDistinctClassInOtherGraph;
import swiss.sib.swissprot.voidcounter.sparql.IsSourceClassLinkedToTargetClass;
import swiss.sib.swissprot.voidcounter.sparql.IsSourceClassLinkedToTargetClasses;
import swiss.sib.swissprot.voidcounter.sparql.TripleCount;

public class VirtuosoCounters implements Counters {
	private final ConcurrentMap<String, Roaring64NavigableMap> distinctSubjectIris;
	private final ConcurrentMap<String, Roaring64NavigableMap> distinctObjectIris;

	public VirtuosoCounters(ConcurrentMap<String, Roaring64NavigableMap> distinctSubjectIris,
			ConcurrentMap<String, Roaring64NavigableMap> distinctObjectIris) {
		super();
		this.distinctSubjectIris = distinctSubjectIris;
		this.distinctObjectIris = distinctObjectIris;
	}

	@Override
	public QueryCallable<Long> countDistinctBnodeSubjectsInAgraph(CommonVariables cv) {
		return new CountDistinctBnodeSubjectsInAGraph(cv);
	}

	public QueryCallable<Set<String>> findAllGraphs(CommonVariables cv, AtomicInteger scheduledQueries) {
		return new FindGraphs(cv, VIRTUOSO, scheduledQueries);
	}

	public QueryCallable<?> countDistinctIriSubjectsAndObjectsInAGraph(CommonVariables cv) {
		return new CountDistinctIriSubjectsAndObjectsInAGraphVirtuoso(cv, distinctSubjectIris, distinctObjectIris);
	}

	@Override
	public QueryCallable<Long> countDistinctBnodeObjectsInDefaultGraph(CommonVariables cv) {
		return new CountDistinctBnodeObjectsInAllGraphs(cv);
	}

	@Override
	public QueryCallable<Long> countDistinctIriObjectsForDefaultGraph(CommonVariables cv) {
		return new CountDistinctIriObjectsInDefaultGraph(cv);
	}

	@Override
	public QueryCallable<Long> countDistinctLiteralObjectsForDefaultGraph(CommonVariables cv) {
		return new CountDistinctLiteralObjectsInDefaultGraph(cv);
	}

	@Override
	public QueryCallable<Long> countDistinctIriSubjectsForDefaultGraph(CommonVariables cv) {
		return new CountDistinctIriSubjectsInDefaultGraph(cv);
	}

	@Override
	public QueryCallable<Long> countDistinctIriSubjectsInAGraph(CommonVariables cvgd) {
		return new CountDistinctIriSubjectsInAGraphVirtuoso(cvgd, distinctSubjectIris);
	}

	@Override
	public QueryCallable<Long> countDistinctBnodeSubjectsInDefaultGraph(CommonVariables cv) {
		return new CountDistinctBnodeSubjectsInDefaultGraph(cv);
	}

	@Override
	public QueryCallable<Exception> findPredicatesAndClasses(CommonVariables cv,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, Set<IRI> knownPredicates,
			ReadWriteLock rwLock, String classExclusion) {
		return new FindPredicatesAndClasses(cv, schedule, knownPredicates, rwLock, classExclusion, this);
	}

	@Override
	public QueryCallable<List<PredicatePartition>> findPredicates(CommonVariables cv, Set<IRI> knownPredicates,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule) {
		return new FindPredicatesAndCountObjects(cv, knownPredicates, schedule, null, VIRTUOSO, this);
	}

	@Override
	public QueryCallable<List<ClassPartition>> findDistinctClassses(CommonVariables cv,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, String classExclusion) {
		return new FindDistinctClassses(cv, schedule, classExclusion, null, VIRTUOSO);
	}

	@Override
	public QueryCallable<Long> countDistinctLiteralObjects(CommonVariables cv) {
		return new CountDistinctLiteralObjects(cv);
	}

	@Override
	public QueryCallable<Long> countDistinctBnodeSubjects(CommonVariables cv) {
		return new CountDistinctBnodeSubjectsInAGraph(cv);
	}

	@Override
	public QueryCallable<Long> triples(CommonVariables cv) {
		return new TripleCount(cv, VIRTUOSO);
	}

	@Override
	public QueryCallable<Long> isSourceClassLinkedToTargetClass(CommonVariables cv, ClassPartition target,
			PredicatePartition predicatePartition, ClassPartition source) {
		return new IsSourceClassLinkedToTargetClass(cv, target, predicatePartition, source, VIRTUOSO);
	}

	@Override
	public QueryCallable<List<LinkSetToOtherGraph>> isSourceClassLinkedToDistinctClassInOtherGraph(CommonVariables cv,
			PredicatePartition predicatePartition, ClassPartition source, GraphDescription og,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, String classExclusion) {
		return new IsSourceClassLinkedToDistinctClassInOtherGraph(cv, predicatePartition, source, og, schedule,
				classExclusion, this, VIRTUOSO);
	}

	@Override
	public QueryCallable<Set<IRI>> findDataTypeIfNoClassOrDtKnown(CommonVariables cv,
			PredicatePartition predicatePartition, ClassPartition source) {
		return new FindDataTypeIfNoClassOrDtKnown(cv, predicatePartition, source);
	}

	@Override
	public QueryCallable<Exception> findPredicateLinkSets(CommonVariables cv, Set<ClassPartition> classes,
			PredicatePartition predicate, ClassPartition source,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, String classExclusion) {
		return new FindPredicateLinkSets(cv, classes, predicate, source, schedule, classExclusion, this, VIRTUOSO);
	}

	@Override
	public QueryCallable<Set<ObjectPartition>> findNamedIndividualObjectSubjectForPredicateInGraph(CommonVariables cv,
			PredicatePartition predicatePartition, ClassPartition source) {
		return new FindNamedIndividualObjectSubjectForPredicateInGraph(cv, predicatePartition, source, VIRTUOSO);
	}

	@Override
	public QueryCallable<List<PredicatePartition>> findPredicatesAndCountObjects(CommonVariables cv, Set<IRI> knownPredicates,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule,
			Supplier<QueryCallable<?>> onFoundPredicates) {
		return new FindPredicatesAndCountObjects(cv, knownPredicates, schedule, onFoundPredicates, VIRTUOSO, this);
	}

	@Override
	public QueryCallable<List<ClassPartition>> findDistinctClassses(CommonVariables cv,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, String classExclusion,
			Supplier<QueryCallable<?>> onFoundClasses) {
		return new FindDistinctClassses(cv, schedule, classExclusion, onFoundClasses, VIRTUOSO);
	}

	@Override
	public QueryCallable<Long> countUniqueSubjectPerPredicateInGraph(CommonVariables cv,
			PredicatePartition predicatePartition) {
		return new CountUniqueSubjectPerPredicateInGraph(cv, predicatePartition);
	}

	@Override
	public QueryCallable<Long> countUniqueObjectsPerPredicateInGraph(CommonVariables cv,
			PredicatePartition predicatePartition) {
		return new CountUniqueObjectsPerPredicateInGraph(cv, predicatePartition);
	}

	@Override
	public QueryCallable<Long> countTriplesLinkingTwoTypesInDifferentGraphs(CommonVariables cv, LinkSetToOtherGraph ls,
			PredicatePartition pp) {
		return new CountTriplesLinkingTwoTypesInDifferentGraphs(cv, ls, pp, VIRTUOSO);
	}
	
	@Override
	public QueryCallable<Map<IRI, Long>> isSourceClassLinkedToTargetClasses(CommonVariables cv,
			Set<ClassPartition> targetClasses, PredicatePartition predicatePartition, ClassPartition source) {
		return new IsSourceClassLinkedToTargetClasses(cv, targetClasses, predicatePartition, source, VIRTUOSO);
	}
}
