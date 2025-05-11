package swiss.sib.swissprot.voidcounter.sparql;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.Counters;
import swiss.sib.swissprot.voidcounter.QueryCallable;

public final class FindPredicatesAndClasses extends QueryCallable<Exception> {
	private static final Logger log = LoggerFactory.getLogger(FindPredicatesAndClasses.class);
	private final Function<QueryCallable<?>, CompletableFuture<Exception>> schedule;
	private final Set<IRI> knownPredicates;
	private final ReadWriteLock rwLock;
	private final String classExclusion;
	private final CommonVariables cv;
	private final Counters counters;

	public FindPredicatesAndClasses(CommonVariables cv,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, Set<IRI> knownPredicates,
			ReadWriteLock rwLock,  String classExclusion, Counters counters) {
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;
		this.schedule = schedule;
		this.knownPredicates = knownPredicates;
		this.rwLock = rwLock;
		this.classExclusion = classExclusion;
		this.counters = counters;
	}

	@Override
	protected void logStart() {
		log.debug("Scheduling finding predicates and classes for {}", cv.gd().getGraphName());

	}

	@Override
	protected void logEnd() {
		log.debug("Scheduled finding predicates and classes for {}", cv.gd().getGraphName());

	}

	@Override
	protected Exception run(RepositoryConnection connection) throws Exception {
		Supplier<QueryCallable<?>> onFoundClasses = () -> new FindClassPredicatePairs(cv, rwLock, classExclusion, schedule, counters);
		Supplier<QueryCallable<?>> onFoundPredicates = () -> counters.findDistinctClassses(cv, schedule,
				classExclusion, onFoundClasses);
		schedule.apply(counters.findPredicatesAndCountObjects(cv, knownPredicates, schedule, onFoundPredicates));

		return null;
	}

	@Override
	protected Logger getLog() {
		return log;
	}
	
	private static class FindClassPredicatePairs extends QueryCallable<Void> {
		private static final Logger log = LoggerFactory.getLogger(FindClassPredicatePairs.class);
		private final ReadWriteLock rwLock;
		private final String classExclusion;
		private final Function<QueryCallable<?>, CompletableFuture<Exception>> schedule;
		private final CommonVariables cv;
		private final Counters counters;

		public FindClassPredicatePairs(CommonVariables cv,
				ReadWriteLock rwLock, String classExclusion,
				Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, Counters counters) {
			super(cv.repository(), cv.limiter(), cv.finishedQueries());
			this.cv = cv;
			this.rwLock = rwLock;
			this.classExclusion = classExclusion;
			this.schedule = schedule;
			this.counters = counters;
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
				classes = new HashSet<>(cv.gd().getClasses());
				predicates = new HashSet<>(cv.gd().getPredicates());
			} finally {
				readLock.unlock();
			}
		
			for (PredicatePartition predicate : predicates) {
				if (!RDF.TYPE.equals(predicate.getPredicate())) {
					for (ClassPartition source : classes) {
						schedule.apply(counters.findPredicateLinkSets(cv, classes, predicate, source, schedule,
								classExclusion));
					}
				}
			}
			return null;
		}

		@Override
		protected void set(Void t) {

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