package swiss.sib.swissprot.voidcounter.sparql;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;

import org.eclipse.rdf4j.model.IRI;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.FindGraphs;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.LinkSetToOtherGraph;
import swiss.sib.swissprot.servicedescription.ObjectPartition;
import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.Counters;
import swiss.sib.swissprot.voidcounter.QueryCallable;

public class SparqlCounters implements Counters {
	private final OptimizeFor optimizeFor;

	public SparqlCounters(OptimizeFor optimizeFor) {
		this.optimizeFor = optimizeFor;
	}

	@Override
	public QueryCallable<Long> countDistinctBnodeSubjectsInAgraph(CommonVariables cv) {
		return new CountDistinctBnodeSubjectsInAGraph(cv, optimizeFor);
	}

	public QueryCallable<Set<String>> findAllGraphs(CommonVariables cv) {
		return new FindGraphs(cv, optimizeFor);
	}

	public QueryCallable<?> countDistinctIriSubjectsAndObjectsInAGraph(CommonVariables cv) {
		return new CountDistinctIriSubjectsAndObjectsInAGraph(cv, optimizeFor);
	}

	@Override
	public QueryCallable<Long> countDistinctBnodeObjectsInDefaultGraph(CommonVariables cv) {
		return new CountDistinctBnodeObjectsInDefaultGraph(cv, optimizeFor);
	}

	@Override
	public QueryCallable<Long> countDistinctIriObjectsForDefaultGraph(CommonVariables cv) {
		return new CountDistinctIriObjectsInDefaultGraph(cv, optimizeFor);
	}

	@Override
	public QueryCallable<Long> countDistinctLiteralObjectsForDefaultGraph(CommonVariables cv) {
		return new CountDistinctLiteralObjectsInDefaultGraph(cv, optimizeFor);
	}

	@Override
	public QueryCallable<Long> countDistinctIriSubjectsForDefaultGraph(CommonVariables cv) {
		return new CountDistinctIriSubjectsForDefaultGraph(cv, optimizeFor);
	}

	@Override
	public QueryCallable<Long> countDistinctIriSubjectsInAGraph(CommonVariables cvgd) {
		return new CountDistinctIriSubjectsInAGraph(cvgd, optimizeFor);
	}

	@Override
	public QueryCallable<Long> countDistinctBnodeSubjectsInDefaultGraph(CommonVariables cv) {
		return new CountDistinctBnodeSubjectsInDefaultGraph(cv, optimizeFor);
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
		return new FindPredicatesAndCountObjects(cv, knownPredicates, schedule, null, optimizeFor, this);
	}

	@Override
	public QueryCallable<List<ClassPartition>> findDistinctClassses(CommonVariables cv,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, String classExclusion) {
		return new FindDistinctClassses(cv, schedule, classExclusion, null, optimizeFor);
	}

	@Override
	public QueryCallable<Long> countDistinctLiteralObjects(CommonVariables cv) {
		return new CountDistinctLiteralObjects(cv, optimizeFor);
	}

	@Override
	public QueryCallable<Long> countDistinctBnodeSubjects(CommonVariables cv) {
		return new CountDistinctBnodeSubjectsInDefaultGraph(cv, optimizeFor);
	}

	@Override
	public QueryCallable<Long> triples(CommonVariables cv) {
		return new TripleCount(cv, optimizeFor);
	}

	@Override
	public QueryCallable<Long> isSourceClassLinkedToTargetClass(CommonVariables cv, ClassPartition target,
			PredicatePartition predicatePartition, ClassPartition source) {
		return new IsSourceClassLinkedToTargetClass(cv, target, predicatePartition, source, optimizeFor);
	}

	@Override
	public QueryCallable<List<LinkSetToOtherGraph>> isSourceClassLinkedToDistinctClassInOtherGraph(CommonVariables cv,
			PredicatePartition predicatePartition, ClassPartition source, GraphDescription og,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, String classExclusion) {
		return new IsSourceClassLinkedToDistinctClassInOtherGraph(cv, predicatePartition, source, og, schedule,
				classExclusion, this, optimizeFor);
	}

	@Override
	public QueryCallable<Set<IRI>> findDataTypeIfNoClassOrDtKnown(CommonVariables cv,
			PredicatePartition predicatePartition, ClassPartition source) {
		return new FindDataTypeIfNoClassOrDtKnown(cv, predicatePartition, source, optimizeFor);
	}

	@Override
	public QueryCallable<Exception> findPredicateLinkSets(CommonVariables cv, Set<ClassPartition> classes,
			PredicatePartition predicate, ClassPartition source,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, String classExclusion) {
		return new FindPredicateLinkSets(cv, classes, predicate, source, schedule, classExclusion, this, optimizeFor);
	}

	@Override
	public QueryCallable<Set<ObjectPartition>> findNamedIndividualObjectSubjectForPredicateInGraph(CommonVariables cv,
			PredicatePartition predicatePartition, ClassPartition source) {
		return new FindNamedIndividualObjectSubjectForPredicateInGraph(cv, predicatePartition, source, optimizeFor);
	}

	@Override
	public QueryCallable<List<PredicatePartition>> findPredicatesAndCountObjects(CommonVariables cv, Set<IRI> knownPredicates,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule,
			Supplier<QueryCallable<?>> onFoundPredicates) {
		return new FindPredicatesAndCountObjects(cv, knownPredicates, schedule, onFoundPredicates, optimizeFor, this);
	}

	@Override
	public QueryCallable<List<ClassPartition>> findDistinctClassses(CommonVariables cv,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, String classExclusion,
			Supplier<QueryCallable<?>> onFoundClasses) {
		return new FindDistinctClassses(cv, schedule, classExclusion, onFoundClasses, optimizeFor);
	}

	@Override
	public QueryCallable<Long> countUniqueSubjectPerPredicateInGraph(CommonVariables cv,
			PredicatePartition predicatePartition) {
		return new CountUniqueSubjectPerPredicateInGraph(cv, predicatePartition, optimizeFor);
	}

	@Override
	public QueryCallable<Long> countUniqueObjectsPerPredicateInGraph(CommonVariables cv,
			PredicatePartition predicatePartition) {
		return new CountUniqueObjectsPerPredicateInGraph(cv, predicatePartition, optimizeFor);
	}

	@Override
	public QueryCallable<Long> countTriplesLinkingTwoTypesInDifferentGraphs(CommonVariables cv, LinkSetToOtherGraph ls,
			PredicatePartition pp) {
		return new CountTriplesLinkingTwoTypesInDifferentGraphs(cv, ls, pp, optimizeFor);
	}

	@Override
	public QueryCallable<Map<IRI, Long>> isSourceClassLinkedToTargetClasses(CommonVariables cv,
			Set<ClassPartition> targetClasses, PredicatePartition predicatePartition, ClassPartition source) {
		return new IsSourceClassLinkedToTargetClasses(cv, targetClasses, predicatePartition, source, optimizeFor);
	}
}
