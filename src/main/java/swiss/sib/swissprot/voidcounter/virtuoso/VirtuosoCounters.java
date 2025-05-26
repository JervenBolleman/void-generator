package swiss.sib.swissprot.voidcounter.virtuoso;

import static swiss.sib.swissprot.servicedescription.OptimizeFor.VIRTUOSO;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.eclipse.rdf4j.model.IRI;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.FindGraphs;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.LinkSetToOtherGraph;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.voidcounter.CommonGraphVariables;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.Counters;
import swiss.sib.swissprot.voidcounter.QueryCallable;
import swiss.sib.swissprot.voidcounter.Variables;
import swiss.sib.swissprot.voidcounter.sparql.CountTriplesLinkingTwoTypesInDifferentGraphs;
import swiss.sib.swissprot.voidcounter.sparql.FindDistinctClassses;
import swiss.sib.swissprot.voidcounter.sparql.FindNamedIndividualObjectSubjectForPredicateInGraph;
import swiss.sib.swissprot.voidcounter.sparql.FindPredicateLinkSets;
import swiss.sib.swissprot.voidcounter.sparql.FindPredicatesAndClasses;
import swiss.sib.swissprot.voidcounter.sparql.FindPredicatesAndCountObjects;
import swiss.sib.swissprot.voidcounter.sparql.IsSourceClassLinkedToDistinctClassInOtherGraph;
import swiss.sib.swissprot.voidcounter.sparql.IsSourceClassLinkedToDistinctClassInOtherGraphs;
import swiss.sib.swissprot.voidcounter.sparql.IsSourceClassLinkedToTargetClass;
import swiss.sib.swissprot.voidcounter.sparql.TripleCount;

public class VirtuosoCounters implements Counters {
	private final ConcurrentMap<String, Roaring64NavigableMap> distinctSubjectIris;
	private final ConcurrentMap<String, Roaring64NavigableMap> distinctObjectIris;
	private final Function<QueryCallable<?, ? extends Variables>, CompletableFuture<Exception>> schedule;

	public VirtuosoCounters(ConcurrentMap<String, Roaring64NavigableMap> distinctSubjectIris,
			ConcurrentMap<String, Roaring64NavigableMap> distinctObjectIris, Function<QueryCallable<?, ? extends Variables>, CompletableFuture<Exception>> schedule) {
		super();
		this.distinctSubjectIris = distinctSubjectIris;
		this.distinctObjectIris = distinctObjectIris;
		this.schedule = schedule;
	}

	@Override
	public void countDistinctBnodeSubjectsInAgraph(CommonGraphVariables cv) {
		schedule(new CountDistinctBnodeSubjectsInAGraph(cv));
	}

	public CompletableFuture<Exception> findAllGraphs(CommonVariables cv) {
		return schedule(new FindGraphs(cv, VIRTUOSO));
	}

	public void countDistinctIriSubjectsAndObjectsInAGraph(CommonGraphVariables cv) {
		schedule(new CountDistinctIriSubjectsAndObjectsInAGraphVirtuoso(cv, distinctSubjectIris, distinctObjectIris));
	}

	@Override
	public void countDistinctBnodeObjectsInDefaultGraph(CommonVariables cv) {
		schedule(new CountDistinctBnodeObjectsInAllGraphs(cv));
	}

	@Override
	public void countDistinctIriObjectsForDefaultGraph(CommonVariables cv) {
		schedule(new CountDistinctIriObjectsInDefaultGraph(cv));
	}

	@Override
	public void countDistinctLiteralObjectsForDefaultGraph(CommonVariables cv) {
		schedule(new CountDistinctLiteralObjectsInDefaultGraph(cv));
	}

	@Override
	public void countDistinctIriSubjectsForDefaultGraph(CommonVariables cv) {
		schedule(new CountDistinctIriSubjectsInDefaultGraph(cv));
	}

	@Override
	public void countDistinctIriSubjectsInAGraph(CommonGraphVariables cvgd) {
		schedule(new CountDistinctIriSubjectsInAGraphVirtuoso(cvgd, distinctSubjectIris));
	}

	@Override
	public void countDistinctBnodeSubjectsInDefaultGraph(CommonVariables cv) {
		schedule(new CountDistinctBnodeSubjectsInDefaultGraph(cv));
	}

	@Override
	public void findPredicatesAndClasses(CommonGraphVariables cv,
			Set<IRI> knownPredicates, String classExclusion) {
		schedule(new FindPredicatesAndClasses(cv, knownPredicates, classExclusion, this));
	}

	@Override
	public void findPredicates(CommonGraphVariables cv, Set<IRI> knownPredicates) {
		schedule(new FindPredicatesAndCountObjects(cv, knownPredicates, null, VIRTUOSO, this));
	}

	@Override
	public void findDistinctClassses(CommonGraphVariables cv, String classExclusion) {
		schedule(new FindDistinctClassses(cv, classExclusion, null, VIRTUOSO, this));
	}

	@Override
	public void countDistinctLiteralObjects(CommonGraphVariables cv) {
		schedule(new CountDistinctLiteralObjects(cv));
	}

	@Override
	public void countTriplesInNamedGraph(CommonGraphVariables cv) {
		schedule(new TripleCount(cv, VIRTUOSO));
	}

	@Override
	public void isSourceClassLinkedToTargetClass(CommonGraphVariables cv, ClassPartition target,
			PredicatePartition predicatePartition, ClassPartition source) {
		schedule(new IsSourceClassLinkedToTargetClass(cv, target, predicatePartition, source, VIRTUOSO));
	}

	@Override
	public void isSourceClassLinkedToDistinctClassInGraphs(CommonGraphVariables cv,
			PredicatePartition predicatePartition, ClassPartition source, 
			String classExclusion){
		schedule(new IsSourceClassLinkedToDistinctClassInOtherGraphs(cv, predicatePartition, source, 
				classExclusion, VIRTUOSO));
	}
	
	@Override
	public void isSourceClassLinkedToDistinctClassInOtherGraph(CommonGraphVariables cv,
			PredicatePartition predicatePartition, ClassPartition source, GraphDescription og,
			String classExclusion) {
		schedule(new IsSourceClassLinkedToDistinctClassInOtherGraph(cv, predicatePartition, source, og, 
				classExclusion, this, VIRTUOSO));
	}

	@Override
	public void findDataTypePartition(CommonGraphVariables cv,
			PredicatePartition predicatePartition, ClassPartition source) {
		schedule(new FindDataTypeIfNoClassOrDtKnown(cv, predicatePartition, source));
	}

	@Override
	public void findPredicateLinkSets(CommonGraphVariables cv, Set<ClassPartition> classes,
			PredicatePartition predicate, ClassPartition source,
			String classExclusion) {
		schedule(new FindPredicateLinkSets(cv, classes, predicate, source, classExclusion, this, VIRTUOSO));
	}

	@Override
	public void findNamedIndividualObjectSubjectForPredicateInGraph(CommonGraphVariables cv,
			PredicatePartition predicatePartition, ClassPartition source) {
		schedule(new FindNamedIndividualObjectSubjectForPredicateInGraph(cv, predicatePartition, source, VIRTUOSO));
	}

	@Override
	public void findPredicatesAndCountObjects(CommonGraphVariables cv, Set<IRI> knownPredicates,
			Consumer<CommonGraphVariables> onFoundPredicates) {
		schedule(new FindPredicatesAndCountObjects(cv, knownPredicates, onFoundPredicates, VIRTUOSO, this));
	}

	@Override
	public void findDistinctClassses(CommonGraphVariables cv,
			String classExclusion,
			Supplier<QueryCallable<?, CommonGraphVariables>> onFoundClasses) {
		schedule(new FindDistinctClassses(cv, classExclusion, onFoundClasses, VIRTUOSO, this));
	}

	@Override
	public void countUniqueSubjectPerPredicateInGraph(CommonGraphVariables cv,
			PredicatePartition predicatePartition) {
		schedule(new CountUniqueSubjectPerPredicateInGraph(cv, predicatePartition));
	}

	@Override
	public void countUniqueObjectsPerPredicateInGraph(CommonGraphVariables cv,
			PredicatePartition predicatePartition) {
		schedule(new CountUniqueObjectsPerPredicateInGraph(cv, predicatePartition));
	}

	@Override
	public void countTriplesLinkingTwoTypesInDifferentGraphs(CommonGraphVariables cv, LinkSetToOtherGraph ls,
			PredicatePartition pp) {
		schedule(new CountTriplesLinkingTwoTypesInDifferentGraphs(cv, ls, pp, VIRTUOSO));
	}
	
	@Override
	public boolean allInUnionGraph() {
		return VIRTUOSO.allInUnionGraph();
	}

	@Override
	public void countDistinctIriSubjectsAndObjectsInDefaultGraph(CommonVariables cv) {
		throw new IllegalStateException("Virtuoso has a default graph, should not use this");
	}

	@Override
	public void countDistinctIriObjectsInAGraph(CommonGraphVariables cvgd) {
		schedule(new CountDistinctIriObjectsInAGraphVirtuoso(cvgd, distinctObjectIris));
	}

	@Override
	public void countDistinctBnodeObjectsInAGraph(CommonGraphVariables gdcv) {
		schedule(new CountDistinctBnodeObjectsInAGraphVirtuoso(gdcv, distinctObjectIris));
	}

	@Override
	public CompletableFuture<Exception> schedule(QueryCallable<?, ? extends Variables> toRun) {
		return schedule.apply(toRun);
	}
}
