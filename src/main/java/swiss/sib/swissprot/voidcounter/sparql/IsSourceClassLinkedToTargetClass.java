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
import swiss.sib.swissprot.voidcounter.CommonGraphVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

public final class IsSourceClassLinkedToTargetClass extends QueryCallable<Long, CommonGraphVariables> {
	private static final Logger log = LoggerFactory.getLogger(IsSourceClassLinkedToTargetClass.class);

	private static final String SUBJECTS = "subjects";


	private final String countLinks;
	private final IRI predicate;
	private final ClassPartition target;
	private final PredicatePartition predicatePartition;
	private final ClassPartition source;

	public IsSourceClassLinkedToTargetClass(CommonGraphVariables cv, ClassPartition target,
			PredicatePartition predicatePartition, ClassPartition source, OptimizeFor optimizeFor) {
		super(cv);
		
		this.predicate = predicatePartition.getPredicate();
		this.target = target;
		this.predicatePartition = predicatePartition;
		this.source = source;
		this.countLinks = Helper.loadSparqlQuery("count_subjects_with_a_type_and_predicate_to_type", optimizeFor);
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
		MapBindingSet bs = new MapBindingSet();
		bs.setBinding("sourceType", sourceType);
		bs.setBinding("targetType", targetType);
		bs.setBinding("graph", cv.gd().getGraph());
		bs.setBinding("predicate", predicate);
		setQuery(countLinks, bs);
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
			cv.save();
		}
	}

	@Override
	protected Logger getLog() {
		return log;
	}
}