package swiss.sib.swissprot.voidcounter.sparql;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

public final class IsSourceClassLinkedToTargetClass extends QueryCallable<Long> {

	private final String countLinks;

	private static final String SUBJECTS = "subjects";

	public static final Logger log = LoggerFactory.getLogger(IsSourceClassLinkedToTargetClass.class);

	private final IRI predicate;
	private final ClassPartition target;
	private final PredicatePartition predicatePartition;
	private final ClassPartition source;

	private final CommonVariables cv;

	public IsSourceClassLinkedToTargetClass(CommonVariables cv, ClassPartition target,
			PredicatePartition predicatePartition, ClassPartition source, OptimizeFor optimizeFor) {
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;
		this.predicate = predicatePartition.getPredicate();
		this.target = target;
		this.predicatePartition = predicatePartition;
		this.source = source;
		this.countLinks = Helper.loadSparqlQuery("count_subjects_with_a_type_and_predicate_to_type",
				optimizeFor);
	}

	@Override
	protected void logStart() {
		log.debug("Checking if {} connected to {} via {} in {}", source.getClazz(), target.getClass(), predicate,
				cv.gd().getGraphName());
	}

	@Override
	protected void logEnd() {
		log.debug("Checked if {} connected to {} via {} in {}", source.getClazz(), target.getClass(), predicate,
				cv.gd().getGraphName());
	}

	@Override
	protected Long run(RepositoryConnection connection) throws Exception {
		final IRI sourceType = source.getClazz();
		final IRI targetType = target.getClazz();
		MapBindingSet tq = new MapBindingSet();
		tq.setBinding("sourceType", sourceType);
		tq.setBinding("targetType", targetType);
		tq.setBinding("graph", cv.gd().getGraph());
		tq.setBinding("predicate", predicate);
		setQuery(countLinks, tq);
		return Helper.getSingleLongFromSparql(getQuery(), connection, SUBJECTS);
	}

	@Override
	protected void set(Long has) {
		if (has > 0) {
			try {
				cv.writeLock().lock();
				final IRI targetType = target.getClazz();
				ClassPartition subTarget = new ClassPartition(targetType);
				subTarget.setTripleCount(has);
				predicatePartition.putClassPartition(subTarget);
			} finally {
				cv.writeLock().unlock();
			}
			cv.saver().accept(cv.sd());
		}
	}

	@Override
	protected Logger getLog() {
		return log;
	}
}