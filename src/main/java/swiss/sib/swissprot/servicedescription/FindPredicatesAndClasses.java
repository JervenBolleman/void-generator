package swiss.sib.swissprot.servicedescription;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Consumer;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.voidcounter.CountDistinctClassses;
import swiss.sib.swissprot.voidcounter.FindPredicateLinkSets;
import swiss.sib.swissprot.voidcounter.FindPredicates;
import swiss.sib.swissprot.voidcounter.QueryCallable;

final class FindPredicatesAndClasses extends QueryCallable<Exception> {
	private static final Logger log = LoggerFactory.getLogger(FindPredicatesAndClasses.class);
	private final GraphDescription gd;
	private final Consumer<QueryCallable<?>> schedule;
	private final Set<IRI> knownPredicates;
	private final ReadWriteLock rwLock;
	private final AtomicInteger finishedQueries;
	private final Consumer<ServiceDescription> saver;
	private final ServiceDescription sd;

	FindPredicatesAndClasses(GraphDescription gd, Repository repository, Consumer<QueryCallable<?>> schedule, Set<IRI> knownPredicates, ReadWriteLock rwLock, Semaphore limit,
			AtomicInteger finishedQueries, Consumer<ServiceDescription> saver,
			ServiceDescription sd) {
		super(repository, limit);
		this.gd = gd;
		this.schedule = schedule;
		this.knownPredicates = knownPredicates;
		this.rwLock = rwLock;
		this.finishedQueries = finishedQueries;
		this.saver = saver;
		this.sd = sd;
	}

	@Override
	protected void logStart() {
		log.info("Scheduling finding predicates and classes for " + gd.getGraphName());
		
	}

	@Override
	protected void logEnd() {
		log.info("Scheduling finding predicates and classes for " + gd.getGraphName());
		
	}

	@Override
	protected Exception run(RepositoryConnection connection) throws Exception {
		final Lock writeLock = rwLock.writeLock();
		Exception call = new FindPredicates(gd, repository, knownPredicates, schedule, writeLock, limiter,
				finishedQueries, saver, sd).call();
		if (call != null)
			return call;

		call = new CountDistinctClassses(gd, repository, writeLock, limiter, finishedQueries, saver,
				schedule, sd).call();
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
					schedule.accept(new FindPredicateLinkSets(repository, classes, predicate, source,
							writeLock, schedule, limiter, gd, finishedQueries, saver, sd));
			}
		}
		return null;
	}

	@Override
	protected void set(Exception t) {
		//do nothing
		
	}

}