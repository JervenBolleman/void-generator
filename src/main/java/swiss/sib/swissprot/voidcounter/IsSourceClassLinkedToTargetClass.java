package swiss.sib.swissprot.voidcounter;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.servicedescription.ServiceDescription;
import swiss.sib.swissprot.servicedescription.sparql.Helper;

public final class IsSourceClassLinkedToTargetClass extends QueryCallable<Long> {

	private static final String COUNT_LINKS_IN_SAME_GRAPH = Helper
			.loadSparqlQuery("count_subjects_with_a_type_and_predicate_to_type");

	private static final String SUBJECTS = "subjects";

	public static final Logger log = LoggerFactory.getLogger(IsSourceClassLinkedToTargetClass.class);

	private final IRI predicate;
	private final ClassPartition target;
	private final PredicatePartition predicatePartition;
	private final ClassPartition source;
	private final GraphDescription gd;
	private final Lock writeLock;

	private final ServiceDescription sd;

	private final Consumer<ServiceDescription> saver;

	public IsSourceClassLinkedToTargetClass(ServiceDescription sd, Repository repository,
			ClassPartition target, PredicatePartition predicatePartition, ClassPartition source, GraphDescription gd,
			Consumer<ServiceDescription> saver, Lock writeLock, Semaphore limiter, AtomicInteger finishedQueries) {
		super(repository, limiter, finishedQueries);
		this.sd = sd;
		this.predicate = predicatePartition.getPredicate();
		this.target = target;
		this.predicatePartition = predicatePartition;
		this.source = source;
		this.gd = gd;
		this.saver = saver;
		this.writeLock = writeLock;
	}

	@Override
	protected void logStart() {
		log.debug("Checking if {} connected to {} via {} in {}", source.getClazz(), target.getClass(), predicate,
				gd.getGraphName());
	}

	@Override
	protected void logEnd() {
		log.debug("Checked if {} connected to {} via {} in {}", source.getClazz(), target.getClass(), predicate,
				gd.getGraphName());
	}

	@Override
	protected Long run(RepositoryConnection connection) throws Exception {
		final IRI sourceType = source.getClazz();
		final IRI targetType = target.getClazz();
		MapBindingSet tq = new MapBindingSet();
		tq.setBinding("sourceType", sourceType);
		tq.setBinding("targetType", targetType);
		tq.setBinding("graph", gd.getGraph());
		tq.setBinding("predicate", predicate);
		setQuery(COUNT_LINKS_IN_SAME_GRAPH, tq);
		return Helper.getSingleLongFromSparql(getQuery(), connection, SUBJECTS);
	}

	@Override
	protected void set(Long has) {
		if (has > 0) {
			try {
				writeLock.lock();
				final IRI targetType = target.getClazz();
				ClassPartition subTarget = new ClassPartition(targetType);
				subTarget.setTripleCount(has);
				predicatePartition.putClassPartition(subTarget);
			} finally {
				writeLock.unlock();
			}
			saver.accept(sd);
		}
	}

	@Override
	protected Logger getLog() {
		return log;
	}
}