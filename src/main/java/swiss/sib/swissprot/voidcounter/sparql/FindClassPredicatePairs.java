package swiss.sib.swissprot.voidcounter.sparql;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.voidcounter.CommonGraphVariables;
import swiss.sib.swissprot.voidcounter.Counters;
import swiss.sib.swissprot.voidcounter.QueryCallable;

class FindClassPredicatePairs extends QueryCallable<Void, CommonGraphVariables> {
	private static final Logger log = LoggerFactory.getLogger(FindClassPredicatePairs.class);
	private final ReadWriteLock rwLock;
	private final String classExclusion;
	private final Counters counters;

	public FindClassPredicatePairs(CommonGraphVariables cv,
			ReadWriteLock rwLock, String classExclusion,
			Counters counters) {
		super(cv);
		this.rwLock = rwLock;
		this.classExclusion = classExclusion;
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
					counters.findPredicateLinkSets(cv, classes, predicate, source, classExclusion);
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