package swiss.sib.swissprot.voidcounter.sparql;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;
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

public final class IsSourceClassLinkedToTargetClasses extends QueryCallable<Map<IRI, Long>> {
	private static final String TARGET_TYPE = "targetType";

	private static final Logger log = LoggerFactory.getLogger(IsSourceClassLinkedToTargetClasses.class);

	private static final String SUBJECTS = "subjects";

	private final String countLinks;
	private final IRI predicate;
	private final Map<IRI, ClassPartition> targets = new HashMap<>();
	private final PredicatePartition predicatePartition;
	private final ClassPartition source;

	private final CommonVariables cv;

	public IsSourceClassLinkedToTargetClasses(CommonVariables cv, Collection<ClassPartition> targets,
			PredicatePartition predicatePartition, ClassPartition source, OptimizeFor optimizeFor) {
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;
		this.predicate = predicatePartition.getPredicate();
		targets.forEach(t -> this.targets.put(t.getClazz(), t));
		this.predicatePartition = predicatePartition;
		this.source = source;
		this.countLinks = Helper.loadSparqlQuery("count_subjects_with_a_type_and_predicate_to_type", optimizeFor);
	}

	@Override
	protected void logStart() {
		if (log.isDebugEnabled())
			log.debug("Checking if {} connected to {} via {} in {}", source.getClazz(), targetList(), predicate,
					cv.gd().getGraphName());
	}

	private String targetList() {
		return targets.keySet().stream().map(IRI::stringValue).collect(Collectors.joining(","));
	}

	@Override
	protected void logEnd() {
		if (log.isDebugEnabled())
			log.debug("Checked if {} connected to {} via {} in {}", source.getClazz(), targetList(), predicate,
					cv.gd().getGraphName());
	}

	@Override
	protected Map<IRI, Long> run(RepositoryConnection connection) throws Exception {
		final IRI sourceType = source.getClazz();
		MapBindingSet ibs = new MapBindingSet();
		ibs.setBinding("sourceType", sourceType);
		ibs.setBinding("graph", cv.gd().getGraph());
		ibs.setBinding("predicate", predicate);
		setQuery(countLinks, ibs);
		Map<IRI, Long> resultMap = new HashMap<>();
		try (TupleQueryResult result = Helper.runTupleQuery(getQuery(), connection)) {
			while (result.hasNext()) {
				BindingSet bs = result.next();
				long count = 0;
				// This is a workaround for a qlever bug where we might not have a binding
				// for the subjects but it should be there
				if (bs.getBinding(SUBJECTS) != null) {
					((Literal) bs.getBinding(SUBJECTS).getValue()).longValue();
				}
				IRI targetValue = (IRI) bs.getBinding(TARGET_TYPE).getValue();
				resultMap.put(targetValue, count);
			}
		}
		return resultMap;
	}

	@Override
	protected void set(Map<IRI, Long> has) {
		if (!has.isEmpty()) {
			try {
				cv.writeLock().lock();
				for (var h : has.entrySet()) {
					final IRI targetType = h.getKey();
					ClassPartition subTarget = new ClassPartition(targetType);
					subTarget.setTripleCount(h.getValue());
					predicatePartition.putClassPartition(subTarget);
				}
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