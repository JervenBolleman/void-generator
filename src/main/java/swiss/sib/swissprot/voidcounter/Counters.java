package swiss.sib.swissprot.voidcounter;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.eclipse.rdf4j.model.IRI;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.LinkSetToOtherGraph;
import swiss.sib.swissprot.servicedescription.PredicatePartition;

public interface Counters {

	CompletableFuture<Exception> findAllGraphs(CommonVariables cv);

	void countDistinctBnodeSubjectsInAgraph(CommonGraphVariables cv);

	void countDistinctBnodeObjectsInDefaultGraph(CommonVariables cv);

	void countDistinctIriObjectsForDefaultGraph(CommonVariables cv);

	void countDistinctLiteralObjectsForDefaultGraph(CommonVariables cv);

	void countDistinctIriSubjectsForDefaultGraph(CommonVariables cv);

	void countDistinctIriSubjectsInAGraph(CommonGraphVariables cvgd);

	void countDistinctBnodeSubjectsInDefaultGraph(CommonVariables cv);

	void findPredicatesAndClasses(CommonGraphVariables cv,
			Set<IRI> knownPredicates,
			ReadWriteLock rwLock, String classExclusion);

	void findPredicates(CommonGraphVariables cv, Set<IRI> knownPredicates);

	void findDistinctClassses(CommonGraphVariables cv, String classExclusion);

	void countDistinctLiteralObjects(CommonGraphVariables cv);

	void countTriplesInNamedGraph(CommonGraphVariables cv);

	void countDistinctIriSubjectsAndObjectsInAGraph(CommonGraphVariables cv);
	
	void countDistinctIriSubjectsAndObjectsInDefaultGraph(CommonVariables cv);

	void isSourceClassLinkedToTargetClass(CommonGraphVariables cv, ClassPartition target,
			PredicatePartition predicatePartition, ClassPartition source);

	void isSourceClassLinkedToDistinctClassInGraphs(CommonGraphVariables cv,
			PredicatePartition predicatePartition, ClassPartition source, 
			String classExclusion);
	
	void isSourceClassLinkedToDistinctClassInOtherGraph(CommonGraphVariables cv,
			PredicatePartition predicatePartition, ClassPartition source, GraphDescription og,
			String classExclusion);

	void findDataTypePartition(CommonGraphVariables cv, PredicatePartition predicatePartition,
			ClassPartition source);

	void findPredicateLinkSets(CommonGraphVariables cv, Set<ClassPartition> classes,
			PredicatePartition predicate, ClassPartition source,
			String classExclusion);

	void findNamedIndividualObjectSubjectForPredicateInGraph(CommonGraphVariables cv,
			PredicatePartition predicatePartition, ClassPartition source);

	void findPredicatesAndCountObjects(CommonGraphVariables cv, Set<IRI> knownPredicates,
			Consumer<CommonGraphVariables> onFoundPredicates);

	void findDistinctClassses(CommonGraphVariables cv,
			String classExclusion, Supplier<QueryCallable<?, CommonGraphVariables>> onFoundClasses);

	void countUniqueSubjectPerPredicateInGraph(CommonGraphVariables cv, PredicatePartition predicatePartition);

	void countUniqueObjectsPerPredicateInGraph(CommonGraphVariables cv, PredicatePartition predicatePartition);

	void countTriplesLinkingTwoTypesInDifferentGraphs(CommonGraphVariables cv, LinkSetToOtherGraph ls,
			PredicatePartition pp);

	boolean allInUnionGraph();

	void countDistinctIriObjectsInAGraph(CommonGraphVariables cvgd);

	void countDistinctBnodeObjectsInAGraph(CommonGraphVariables gdcv);
	
	CompletableFuture<Exception> schedule(QueryCallable<?, ? extends Variables> toRun);
}