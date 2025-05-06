package swiss.sib.swissprot.servicedescription;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.voidcounter.FindDistinctClassses;
import swiss.sib.swissprot.voidcounter.FindPredicateLinkSets;
import swiss.sib.swissprot.voidcounter.FindPredicates;
import swiss.sib.swissprot.voidcounter.QueryCallable;

final class FindPredicatesAndClasses extends QueryCallable<Exception> {
	private static final Logger log = LoggerFactory.getLogger(FindPredicatesAndClasses.class);
	private final GraphDescription gd;
	private final Function<QueryCallable<?>, CompletableFuture<Exception>> schedule;
	private final Set<IRI> knownPredicates;
	private final ReadWriteLock rwLock;
	private final AtomicInteger finishedQueries;
	private final Consumer<ServiceDescription> saver;
	private final ServiceDescription sd;
	private final String classExclusion;
	private final boolean preferGroupBy;

	FindPredicatesAndClasses(GraphDescription gd, Repository repository,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, Set<IRI> knownPredicates,
			ReadWriteLock rwLock, Semaphore limit, AtomicInteger finishedQueries, Consumer<ServiceDescription> saver,
			ServiceDescription sd, String classExclusion, boolean preferGroupBy) {
		super(repository, limit, finishedQueries);
		this.gd = gd;
		this.schedule = schedule;
		this.knownPredicates = knownPredicates;
		this.rwLock = rwLock;
		this.finishedQueries = finishedQueries;
		this.saver = saver;
		this.sd = sd;
		this.classExclusion = classExclusion;
		this.preferGroupBy = preferGroupBy;
	}

	@Override
	protected void logStart() {
		log.debug("Scheduling finding predicates and classes for " + gd.getGraphName());

	}

	@Override
	protected void logEnd() {
		log.debug("Scheduled finding predicates and classes for " + gd.getGraphName());

	}

	@Override
	protected Exception run(RepositoryConnection connection) throws Exception {
		final Lock writeLock = rwLock.writeLock();
		Supplier<QueryCallable<?>> onFoundClasses = () -> new FindClassPredicatePairs(repository, limiter,
				finishedQueries, rwLock, saver, gd, classExclusion, schedule, sd);
		Supplier<QueryCallable<?>> onFoundPredicates = () -> {
			return new FindDistinctClassses(gd, repository, writeLock, limiter, finishedQueries, saver, schedule, sd,
					classExclusion, onFoundClasses, preferGroupBy);
		};
		schedule.apply(new FindPredicates(gd, repository, knownPredicates, schedule, writeLock, limiter,
				finishedQueries, saver, sd, onFoundPredicates));

		return null;
	}

	@Override
	protected Logger getLog() {
		return log;
	}
	
	private static class FindClassPredicatePairs extends QueryCallable<Void> {
		private static final Logger log = LoggerFactory.getLogger(FindClassPredicatePairs.class);
		private final ReadWriteLock rwLock;
		private final GraphDescription gd;
		private final Consumer<ServiceDescription> saver;
		private final String classExclusion;
		private final Function<QueryCallable<?>, CompletableFuture<Exception>> schedule;
		private final ServiceDescription sd;

		public FindClassPredicatePairs(Repository repository, Semaphore limiter, AtomicInteger finishedQueries,
				ReadWriteLock rwLock, Consumer<ServiceDescription> saver, GraphDescription gd, String classExclusion,
				Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, ServiceDescription sd) {
			super(repository, limiter, finishedQueries);
			this.rwLock = rwLock;
			this.saver = saver;
			this.gd = gd;
			this.classExclusion = classExclusion;
			this.schedule = schedule;
			this.sd = sd;
		}

		@Override
		protected void logStart() {
			log.debug("Scheduling finding class predicate pairs");

		}

		@Override
		protected void logEnd() {
			log.debug("Scheduled finding class predicate pairs");
		}

		@Override
		protected Void run(RepositoryConnection connection) throws Exception {
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
		
			final Lock writeLock = rwLock.writeLock();
			for (PredicatePartition predicate : predicates) {
				for (ClassPartition source : classes) {
					if (!RDF.TYPE.equals(predicate.getPredicate()))
						schedule.apply(new FindPredicateLinkSets(repository, classes, predicate, source, writeLock,
								schedule, limiter, gd, finishedQueries, saver, sd, classExclusion));
				}
			}
			return null;
		}

		@Override
		protected void set(Void t) {
			// TODO Auto-generated method stub

		}
		
		@Override
		protected Logger getLog() {
			return log;
		}

	}

	@Override
	protected void set(Exception t) {
		// do nothing

	}

}