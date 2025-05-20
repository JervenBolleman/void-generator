package swiss.sib.swissprot.voidcounter.sparql;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.eclipse.rdf4j.model.IRI;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.FindGraphs;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.LinkSetToOtherGraph;
import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.voidcounter.CommonGraphVariables;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.Counters;
import swiss.sib.swissprot.voidcounter.QueryCallable;
import swiss.sib.swissprot.voidcounter.Variables;

public class SparqlCounters implements Counters {
	private final OptimizeFor optimizeFor;
	private final Function<QueryCallable<?, ? extends Variables>, CompletableFuture<Exception>> schedule;

	public SparqlCounters(OptimizeFor optimizeFor,
			Function<QueryCallable<?, ? extends Variables>, CompletableFuture<Exception>> schedule) {
		this.optimizeFor = optimizeFor;
		this.schedule = schedule;
	}

	@Override
	public void countDistinctBnodeSubjectsInAgraph(CommonGraphVariables cv) {
		schedule(new CountDistinctBnodeSubjectsInAGraph(cv, optimizeFor));
	}

	public CompletableFuture<Exception> findAllGraphs(CommonVariables cv) {
		return schedule(new FindGraphs(cv, optimizeFor));
	}

	public void countDistinctIriSubjectsAndObjectsInAGraph(CommonGraphVariables cv) {
		schedule(new CountDistinctIriSubjectsAndObjectsInAGraph(cv, optimizeFor));
	}

	public void countDistinctIriSubjectsAndObjectsInDefaultGraph(CommonVariables cv) {
		schedule(new CountDistinctIriSubjectsAndObjectsInDefaultGraph(cv, optimizeFor));
	}

	@Override
	public void countDistinctBnodeObjectsInDefaultGraph(CommonVariables cv) {
		schedule(new CountDistinctBnodeObjectsInDefaultGraph(cv, optimizeFor));
	}

	@Override
	public void countDistinctIriObjectsForDefaultGraph(CommonVariables cv) {
		schedule(new CountDistinctIriObjectsInDefaultGraph(cv, optimizeFor));
	}

	@Override
	public void countDistinctIriObjectsInAGraph(CommonGraphVariables cv) {
		schedule(new CountDistinctIriObjectsInAGraph(cv, optimizeFor));
	}

	@Override
	public void countDistinctLiteralObjectsForDefaultGraph(CommonVariables cv) {
		schedule(new CountDistinctLiteralObjectsInDefaultGraph(cv, optimizeFor));
	}

	@Override
	public void countDistinctIriSubjectsForDefaultGraph(CommonVariables cv) {
		schedule(new CountDistinctIriSubjectsForDefaultGraph(cv, optimizeFor));
	}

	@Override
	public void countDistinctIriSubjectsInAGraph(CommonGraphVariables cvgd) {
		schedule(new CountDistinctIriSubjectsInAGraph(cvgd, optimizeFor));
	}

	@Override
	public void countDistinctBnodeSubjectsInDefaultGraph(CommonVariables cv) {
		schedule(new CountDistinctBnodeSubjectsInDefaultGraph(cv, optimizeFor));
	}

	@Override
	public void findPredicatesAndClasses(CommonGraphVariables cv, Set<IRI> knownPredicates,
			ReadWriteLock rwLock, String classExclusion) {
		schedule(new FindPredicatesAndClasses(cv, knownPredicates, rwLock, classExclusion, this));
	}

	@Override
	public void findPredicates(CommonGraphVariables cv, Set<IRI> knownPredicates) {
		schedule(new FindPredicatesAndCountObjects(cv, knownPredicates, null, optimizeFor, this));
	}

	@Override
	public void findDistinctClassses(CommonGraphVariables cv, String classExclusion) {
		schedule(new FindDistinctClassses(cv, classExclusion, null, optimizeFor, this));
	}

	@Override
	public void countDistinctLiteralObjects(CommonGraphVariables cv) {
		schedule(new CountDistinctLiteralObjects(cv, optimizeFor));
	}

	@Override
	public void countTriplesInNamedGraph(CommonGraphVariables cv) {
		schedule(new TripleCount(cv, optimizeFor));
	}

	@Override
	public void isSourceClassLinkedToTargetClass(CommonGraphVariables cv, ClassPartition target,
			PredicatePartition predicatePartition, ClassPartition source) {
		schedule(new IsSourceClassLinkedToTargetClass(cv, target, predicatePartition, source, optimizeFor));
	}

	@Override
	public void isSourceClassLinkedToDistinctClassInGraphs(CommonGraphVariables cv,
			PredicatePartition predicatePartition, ClassPartition source, String classExclusion) {
		schedule(new IsSourceClassLinkedToDistinctClassInOtherGraphs(cv, predicatePartition, source, classExclusion,
				optimizeFor));
	}

	@Override
	public void isSourceClassLinkedToDistinctClassInOtherGraph(
			CommonGraphVariables cv, PredicatePartition predicatePartition, ClassPartition source, GraphDescription og,
			String classExclusion) {
		schedule(new IsSourceClassLinkedToDistinctClassInOtherGraph(cv, predicatePartition, source, og, classExclusion,
				this, optimizeFor));
	}

	@Override
	public void findDataTypeIfNoClassOrDtKnown(CommonGraphVariables cv,
			PredicatePartition predicatePartition, ClassPartition source) {
		schedule(new FindDataTypeIfNoClassOrDtKnown(cv, predicatePartition, source, optimizeFor));
	}

	@Override
	public void findPredicateLinkSets(CommonGraphVariables cv, Set<ClassPartition> classes,
			PredicatePartition predicate, ClassPartition source, String classExclusion) {
		schedule(new FindPredicateLinkSets(cv, classes, predicate, source, classExclusion, this, optimizeFor));
	}

	@Override
	public void findNamedIndividualObjectSubjectForPredicateInGraph(
			CommonGraphVariables cv, PredicatePartition predicatePartition, ClassPartition source) {
		schedule(new FindNamedIndividualObjectSubjectForPredicateInGraph(cv, predicatePartition, source, optimizeFor));
	}

	@Override
	public void findPredicatesAndCountObjects(CommonGraphVariables cv,
			Set<IRI> knownPredicates, Consumer<CommonGraphVariables> onFoundPredicates) {
		schedule(new FindPredicatesAndCountObjects(cv, knownPredicates, onFoundPredicates, optimizeFor, this));
	}

	@Override
	public void findDistinctClassses(CommonGraphVariables cv, String classExclusion,
			Supplier<QueryCallable<?, CommonGraphVariables>> onFoundClasses) {
		schedule(new FindDistinctClassses(cv, classExclusion, onFoundClasses, optimizeFor, this));
	}

	@Override
	public void countUniqueSubjectPerPredicateInGraph(CommonGraphVariables cv,
			PredicatePartition predicatePartition) {
		schedule(new CountUniqueSubjectPerPredicateInGraph(cv, predicatePartition, optimizeFor));
	}

	@Override
	public void countUniqueObjectsPerPredicateInGraph(CommonGraphVariables cv,
			PredicatePartition predicatePartition) {
		schedule(new CountUniqueObjectsPerPredicateInGraph(cv, predicatePartition, optimizeFor));
	}

	@Override
	public void countTriplesLinkingTwoTypesInDifferentGraphs(CommonGraphVariables cv,
			LinkSetToOtherGraph ls, PredicatePartition pp) {
		schedule(new CountTriplesLinkingTwoTypesInDifferentGraphs(cv, ls, pp, optimizeFor));
	}

	@Override
	public boolean allInUnionGraph() {
		return optimizeFor.allInUnionGraph();
	}

	@Override
	public void countDistinctBnodeObjectsInAGraph(CommonGraphVariables gdcv) {
		schedule(new CountDistinctBnodeObjectsInAGraph(gdcv, optimizeFor));
	}

	@Override
	public CompletableFuture<Exception> schedule(QueryCallable<?, ?> toRun) {
		return schedule.apply(toRun);
	}
}
