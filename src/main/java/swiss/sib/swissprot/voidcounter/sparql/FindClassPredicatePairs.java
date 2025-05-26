package swiss.sib.swissprot.voidcounter.sparql;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;

import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.voidcounter.CommonGraphVariables;
import swiss.sib.swissprot.voidcounter.Counters;
import swiss.sib.swissprot.voidcounter.QueryCallable;

class FindClassPredicatePairs extends QueryCallable<Exception, CommonGraphVariables> {
	private static final Logger log = LoggerFactory.getLogger(FindClassPredicatePairs.class);
	private final String classExclusion;
	private final Counters counters;
	private final CompletableFuture<Exception>[] future;

	@SafeVarargs
	public FindClassPredicatePairs(CommonGraphVariables cv,
			String classExclusion,
			Counters counters, CompletableFuture<Exception>... future) {
		super(cv);
		this.classExclusion = classExclusion;
		this.counters = counters;
		this.future = future;
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
	protected Exception run(RepositoryConnection connection) {
		Set<ClassPartition> classes;
		Set<PredicatePartition> predicates;
		if (future != null) {
			for (CompletableFuture<Exception> f : future) {
				var join = f.join();
				if (join != null) {
					return join;
				}
			}
		}
		final Lock readLock = cv.readLock();
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
					counters.findPredicateLinkSets(cv, classes, predicate, source, classExclusion);
				}
			}
		}
		return null;
	}

	@Override
	protected void set(Exception t) {

	}
	
	@Override
	public Logger getLog() {
		return log;
	}

}