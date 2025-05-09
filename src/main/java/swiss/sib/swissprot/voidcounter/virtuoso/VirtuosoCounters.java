package swiss.sib.swissprot.voidcounter.virtuoso;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Function;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import swiss.sib.swissprot.servicedescription.FindGraphs;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.Counters;
import swiss.sib.swissprot.voidcounter.QueryCallable;
import swiss.sib.swissprot.voidcounter.sparql.FindDistinctClassses;
import swiss.sib.swissprot.voidcounter.sparql.FindPredicatesAndCountObjects;
import swiss.sib.swissprot.voidcounter.sparql.FindPredicatesAndClasses;
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

	public Set<String> findAllNonVirtuosoGraphs(RepositoryConnection connection, AtomicInteger scheduledQueries,
			AtomicInteger finishedQueries) {
		return FindGraphs.findAllNonVirtuosoGraphs(connection, scheduledQueries, finishedQueries);
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
		return new FindPredicatesAndClasses(cv, schedule, knownPredicates, rwLock, classExclusion);
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
	public QueryCallable<?> triples(CommonVariables cv) {
		return new TripleCount(cv);
	}
}
