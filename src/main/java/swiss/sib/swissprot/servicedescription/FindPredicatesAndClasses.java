package swiss.sib.swissprot.servicedescription;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Consumer;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.repository.Repository;

import swiss.sib.swissprot.voidcounter.CountDistinctClassses;
import swiss.sib.swissprot.voidcounter.CountDistinctShapeDefiningSuperClasses;
import swiss.sib.swissprot.voidcounter.FindPredicateLinkSets;
import swiss.sib.swissprot.voidcounter.FindPredicates;

final class FindPredicatesAndClasses implements Callable<Exception> {
	private final GraphDescription gd;
	private final Repository repository;
	private final ExecutorService execs;
	private final List<Future<Exception>> futures;
	private final Set<IRI> knownPredicates;
	private final ReadWriteLock rwLock;
	private final Semaphore limit;
	private final AtomicInteger scheduledQueries;
	private final AtomicInteger finishedQueries;
	private final Consumer<ServiceDescription> saver;
	private final ServiceDescription sd;

	FindPredicatesAndClasses(GraphDescription gd, Repository repository, ExecutorService execs,
			List<Future<Exception>> futures, Set<IRI> knownPredicates, ReadWriteLock rwLock, Semaphore limit,
			AtomicInteger scheduledQueries, AtomicInteger finishedQueries, Consumer<ServiceDescription> saver,
			ServiceDescription sd) {
		this.gd = gd;
		this.repository = repository;
		this.execs = execs;
		this.futures = futures;
		this.knownPredicates = knownPredicates;
		this.rwLock = rwLock;
		this.limit = limit;
		this.scheduledQueries = scheduledQueries;
		this.finishedQueries = finishedQueries;
		this.saver = saver;
		this.sd = sd;
	}

	@Override
	public Exception call() {
		final Lock writeLock = rwLock.writeLock();
		Exception call = new FindPredicates(gd, repository, knownPredicates, futures, execs, writeLock, limit,
				scheduledQueries, finishedQueries, saver, sd).call();
		if (call != null)
			return call;

		call = new CountDistinctClassses(gd, repository, writeLock, limit, scheduledQueries, finishedQueries, saver,
				sd).call();
		if (call != null)
			return call;
		
		call = new CountDistinctShapeDefiningSuperClasses(gd, repository, writeLock, limit, scheduledQueries, finishedQueries, saver,
				sd).call();
		if (call != null)
			return call;

		Set<ClassPartition> classes;
		Set<PredicatePartition> predicates;

		final Lock readLock = rwLock.readLock();
		try {
			readLock.lock();
			classes = new HashSet<>(gd.getClasses());
			predicates = new HashSet<>(gd.getPredicates());
		} finally {
			readLock.unlock();
		}

		for (PredicatePartition predicate : predicates) {
			for (ClassPartition source : classes) {
				if (!RDF.TYPE.equals(predicate.getPredicate()))
					futures.add(execs.submit(new FindPredicateLinkSets(repository, classes, predicate, source,
							writeLock, futures, limit, execs, gd, scheduledQueries, finishedQueries, saver, sd)));
			}
		}
		return null;
	}

}