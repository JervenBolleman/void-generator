package swiss.sib.swissprot.voidcounter;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;

import org.eclipse.rdf4j.model.IRI;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.LinkSetToOtherGraph;
import swiss.sib.swissprot.servicedescription.ObjectPartition;
import swiss.sib.swissprot.servicedescription.PredicatePartition;

public interface Counters {

	QueryCallable<Set<String>> findAllGraphs(CommonVariables cv);

	QueryCallable<Long> countDistinctBnodeSubjectsInAgraph(CommonVariables cv);

	QueryCallable<Long> countDistinctBnodeObjectsInDefaultGraph(CommonVariables cv);

	QueryCallable<Long> countDistinctIriObjectsForDefaultGraph(CommonVariables cv);

	QueryCallable<Long> countDistinctLiteralObjectsForDefaultGraph(CommonVariables cv);

	QueryCallable<Long> countDistinctIriSubjectsForDefaultGraph(CommonVariables cv);

	QueryCallable<Long> countDistinctIriSubjectsInAGraph(CommonVariables cvgd);

	QueryCallable<Long> countDistinctBnodeSubjectsInDefaultGraph(CommonVariables cv);

	QueryCallable<Exception> findPredicatesAndClasses(CommonVariables cv,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, Set<IRI> knownPredicates,
			ReadWriteLock rwLock, String classExclusion);

	QueryCallable<List<PredicatePartition>> findPredicates(CommonVariables cv, Set<IRI> knownPredicates,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule);

	QueryCallable<List<ClassPartition>> findDistinctClassses(CommonVariables cv,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, String classExclusion);

	QueryCallable<Long> countDistinctLiteralObjects(CommonVariables cv);

	QueryCallable<Long> countDistinctBnodeSubjects(CommonVariables cv);

	QueryCallable<Long> countTriplesInNamedGraph(CommonVariables cv);

	QueryCallable<?> countDistinctIriSubjectsAndObjectsInAGraph(CommonVariables cv);

	QueryCallable<Long> isSourceClassLinkedToTargetClass(CommonVariables cv, ClassPartition target,
			PredicatePartition predicatePartition, ClassPartition source);

	QueryCallable<List<LinkSetToOtherGraph>> isSourceClassLinkedToDistinctClassInGraphs(CommonVariables cv,
			PredicatePartition predicatePartition, ClassPartition source, 
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, String classExclusion);
	
	QueryCallable<List<LinkSetToOtherGraph>> isSourceClassLinkedToDistinctClassInOtherGraph(CommonVariables cv,
			PredicatePartition predicatePartition, ClassPartition source, GraphDescription og,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, String classExclusion);

	QueryCallable<Set<IRI>> findDataTypeIfNoClassOrDtKnown(CommonVariables cv, PredicatePartition predicatePartition,
			ClassPartition source);

	QueryCallable<Exception> findPredicateLinkSets(CommonVariables cv, Set<ClassPartition> classes,
			PredicatePartition predicate, ClassPartition source,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, String classExclusion);

	QueryCallable<Set<ObjectPartition>> findNamedIndividualObjectSubjectForPredicateInGraph(CommonVariables cv,
			PredicatePartition predicatePartition, ClassPartition source);

	QueryCallable<List<PredicatePartition>> findPredicatesAndCountObjects(CommonVariables cv, Set<IRI> knownPredicates,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule,
			Supplier<QueryCallable<?>> onFoundPredicates);

	QueryCallable<List<ClassPartition>> findDistinctClassses(CommonVariables cv, Function<QueryCallable<?>, CompletableFuture<Exception>> schedule,
			String classExclusion, Supplier<QueryCallable<?>> onFoundClasses);

	QueryCallable<Long> countUniqueSubjectPerPredicateInGraph(CommonVariables cv, PredicatePartition predicatePartition);

	QueryCallable<Long> countUniqueObjectsPerPredicateInGraph(CommonVariables cv, PredicatePartition predicatePartition);

	QueryCallable<Long> countTriplesLinkingTwoTypesInDifferentGraphs(CommonVariables cv, LinkSetToOtherGraph ls,
			PredicatePartition pp);
}