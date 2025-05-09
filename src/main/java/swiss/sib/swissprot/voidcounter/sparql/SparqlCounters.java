package swiss.sib.swissprot.voidcounter.sparql;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Function;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.repository.RepositoryConnection;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.FindGraphs;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.LinkSetToOtherGraph;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.Counters;
import swiss.sib.swissprot.voidcounter.QueryCallable;

public class SparqlCounters implements Counters {
	@Override
	public QueryCallable<Long> countDistinctBnodeSubjectsInAgraph(CommonVariables cv) {
		return new CountDistinctBnodeSubjectsInAGraph(cv);
	}

	public Set<String> findAllNonVirtuosoGraphs(RepositoryConnection connection, AtomicInteger scheduledQueries,
			AtomicInteger finishedQueries) {
		return FindGraphs.findAllNonVirtuosoGraphs(connection, scheduledQueries, finishedQueries);
	}

	public QueryCallable<?> countDistinctIriSubjectsAndObjectsInAGraph(CommonVariables cv) {
		return new CountDistinctIriSubjectsAndObjectsInAGraph(cv);
	}

	@Override
	public QueryCallable<Long> countDistinctBnodeObjectsInDefaultGraph(CommonVariables cv) {
		return new CountDistinctBnodeObjectsInDefaultGraph(cv);
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
		return new CountDistinctIriSubjectsForDefaultGraph(cv);
	}

	@Override
	public QueryCallable<Long> countDistinctIriSubjectsInAGraph(CommonVariables cvgd) {
		return new CountDistinctIriSubjectsInAGraph(cvgd);
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
	public QueryCallable<?> findPredicates(CommonVariables cv, Set<IRI> knownPredicates,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule) {
		return new FindPredicatesAndCountObjects(cv, knownPredicates, schedule, null);
	}

	@Override
	public QueryCallable<?> findDistinctClassses(CommonVariables cv,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, String classExclusion) {
		return new FindDistinctClassses(cv, schedule, classExclusion, null);
	}

	@Override
	public QueryCallable<?> countDistinctLiteralObjects(CommonVariables cv) {
		return new CountDistinctLiteralObjects(cv);
	}

	@Override
	public QueryCallable<?> countDistinctBnodeSubjects(CommonVariables cv) {
		return new CountDistinctBnodeSubjectsInAGraph(cv);
	}

	@Override
	public QueryCallable<Long> triples(CommonVariables cv) {
		return new TripleCount(cv);
	}

	@Override
	public QueryCallable<Long> isSourceClassLinkedToTargetClass(CommonVariables cv, ClassPartition target,
			PredicatePartition predicatePartition, ClassPartition source) {
		return new IsSourceClassLinkedToTargetClass(cv, target, predicatePartition, source);
	}

	@Override
	public QueryCallable<List<LinkSetToOtherGraph>> isSourceClassLinkedToDistinctClassInOtherGraph(CommonVariables cv,
			PredicatePartition predicatePartition, ClassPartition source, GraphDescription og,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, String classExclusion) {
		return new IsSourceClassLinkedToDistinctClassInOtherGraph(cv, predicatePartition, source, og, schedule,
				classExclusion);
	}

	@Override
	public QueryCallable<Set<IRI>> findDataTypeIfNoClassOrDtKnown(CommonVariables cv, PredicatePartition predicatePartition,
			ClassPartition source) {
		return new FindDataTypeIfNoClassOrDtKnown(cv, predicatePartition, source);
	}

	@Override
	public QueryCallable<Exception> findPredicateLinkSets(CommonVariables cv, Set<ClassPartition> classes,
			PredicatePartition predicate, ClassPartition source,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, String classExclusion) {
		return new FindPredicateLinkSets(cv, classes, predicate, source, schedule, classExclusion, this);
	}

	@Override
	public QueryCallable<?> findNamedIndividualObjectSubjectForPredicateInGraph(CommonVariables cv,
			PredicatePartition predicatePartition, ClassPartition source) {
		return new FindNamedIndividualObjectSubjectForPredicateInGraph(cv, predicatePartition, source);
	}
}
