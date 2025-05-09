package swiss.sib.swissprot.voidcounter.sparql;

import java.util.HashSet;
import java.util.Set;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.ObjectPartition;
import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

public class FindNamedIndividualObjectSubjectForPredicateInGraph extends QueryCallable<Set<ObjectPartition>> {
	private final String rawQuery;
	private static final Logger log = LoggerFactory
			.getLogger(FindNamedIndividualObjectSubjectForPredicateInGraph.class);

	private final PredicatePartition predicatePartition;

	private final ClassPartition cp;

	private final CommonVariables cv;

	public FindNamedIndividualObjectSubjectForPredicateInGraph(CommonVariables cv,
			PredicatePartition predicatePartition, ClassPartition cp, OptimizeFor optimizeFor) {
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;
		this.predicatePartition = predicatePartition;
		this.cp = cp;
		this.rawQuery = Helper.loadSparqlQuery("count_subjects_with_a_type_and_predicate_to_named_individual",
				optimizeFor);
	}

	@Override
	protected void logStart() {
		log.debug("Finding if {} with predicate partition {} has subject partitions in {}", cp.getClazz(),
				predicatePartition.getPredicate(), cv.gd().getGraphName());
	}

	@Override
	protected void logEnd() {
		if (!predicatePartition.getSubjectPartitions().isEmpty()) {
			log.debug("{} has predicate partition {} with {} subject partition in {}", cp.getClazz(),
					predicatePartition.getPredicate(), predicatePartition.getSubjectPartitions().size(),
					cv.gd().getGraphName());
		} else {
			log.debug("Found no {} with predicate partition {} has subject partitions in {}", cp.getClazz(),
					predicatePartition.getPredicate(), cv.gd().getGraphName());
		}
	}

	@Override
	protected Set<ObjectPartition> run(RepositoryConnection connection) throws Exception {
		MapBindingSet bs = new MapBindingSet();
		bs.setBinding("graph", cv.gd().getGraph());
		bs.setBinding("sourceType", cp.getClazz());
		bs.setBinding("predicate", predicatePartition.getPredicate());
		setQuery(rawQuery, bs);
		Set<ObjectPartition> namedObjects = new HashSet<>();
		try (final TupleQueryResult tr = Helper.runTupleQuery(getQuery(), connection)) {
			while (tr.hasNext()) {
				final BindingSet next = tr.next();
				if (next.hasBinding("target")) {
					Value v = next.getBinding("target").getValue();
					Value count = next.getBinding("count").getValue();

					if (v instanceof IRI i && count instanceof Literal c) {
						ObjectPartition subTarget = new ObjectPartition(i);
						subTarget.setTripleCount(c.longValue());
						namedObjects.add(subTarget);
					}
				}
			}
		}
		return namedObjects;
	}

	@Override
	protected void set(Set<ObjectPartition> namedObjects) {
		try {
			cv.writeLock().lock();
			for (ObjectPartition namedObject : namedObjects) {
				predicatePartition.putSubjectPartition(namedObject);
			}
		} finally {
			cv.writeLock().unlock();
		}
		if (!namedObjects.isEmpty())
			cv.saver().accept(cv.sd());
	}

	@Override
	protected Logger getLog() {
		return log;
	}
}