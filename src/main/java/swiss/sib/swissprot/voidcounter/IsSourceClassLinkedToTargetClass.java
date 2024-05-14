package swiss.sib.swissprot.voidcounter;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.PredicatePartition;

public final class IsSourceClassLinkedToTargetClass extends QueryCallable<Boolean> {

	public static final Logger log = LoggerFactory.getLogger(IsSourceClassLinkedToTargetClass.class);

	private final IRI predicate;
	private final ClassPartition target;
	private final PredicatePartition predicatePartition;
	private final ClassPartition source;
	private final GraphDescription gd;
	private final Lock writeLock;
	private final AtomicInteger finishedQueries;

	public IsSourceClassLinkedToTargetClass(Repository repository, IRI predicate, ClassPartition target,
			PredicatePartition predicatePartition, ClassPartition source, GraphDescription gd, Lock writeLock,
			Semaphore limiter, AtomicInteger scheduledQueries, AtomicInteger finishedQueries) {
		super(repository, limiter);
		this.predicate = predicate;
		this.target = target;
		this.predicatePartition = predicatePartition;
		this.source = source;
		this.gd = gd;
		this.writeLock = writeLock;
		this.finishedQueries = finishedQueries;
		scheduledQueries.incrementAndGet();
	}

	@Override
	protected void logStart() {
		log.debug("Checking if " + source.getClazz() + " connected to " + target.getClass() + " via " + predicate
				+ " in " + gd.getGraphName());
	}

	@Override
	protected void logEnd() {
		log.debug("Checked if" + source.getClazz() + " connected to " + target.getClass() + " via " + predicate + " in "
				+ gd.getGraphName());
	}

	@Override
	protected Boolean run(RepositoryConnection connection) throws Exception {
		try {
			final IRI sourceType = source.getClazz();
			final IRI targetType = target.getClazz();
			final String query = "ASK  { GRAPH <" + gd.getGraphName() + ">{ ?subject a <" + sourceType + "> ; <"
					+ predicate + "> ?target . ?target a <" + targetType + "> }}";
			final BooleanQuery pbq = connection.prepareBooleanQuery(QueryLanguage.SPARQL, query);
			return pbq.evaluate();
		} finally {
			finishedQueries.incrementAndGet();
		}
	}

	@Override
	protected void set(Boolean has) {
		if (has) {
			try {
				writeLock.lock();
				final IRI targetType = target.getClazz();
				ClassPartition subTarget = new ClassPartition(targetType);
				predicatePartition.putClassPartition(subTarget);
			} finally {
				writeLock.unlock();
			}
		}

	}
}