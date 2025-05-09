package swiss.sib.swissprot.voidcounter;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Function;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.repository.RepositoryConnection;

public interface Counters {

	Set<String> findAllNonVirtuosoGraphs(RepositoryConnection connection, AtomicInteger scheduledQueries,
			AtomicInteger finishedQueries);

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

	QueryCallable<?> findPredicates(CommonVariables cv, Set<IRI> knownPredicates,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule);

	QueryCallable<?> findDistinctClassses(CommonVariables cv,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, String classExclusion);

	QueryCallable<?> countDistinctLiteralObjects(CommonVariables cv);

	QueryCallable<?> countDistinctBnodeSubjects(CommonVariables cv);

	QueryCallable<?> triples(CommonVariables cv);

	QueryCallable<?> countDistinctIriSubjectsAndObjectsInAGraph(CommonVariables cv);

}