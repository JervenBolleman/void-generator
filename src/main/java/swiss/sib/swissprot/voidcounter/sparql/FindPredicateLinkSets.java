package swiss.sib.swissprot.voidcounter.sparql;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.LinkSetToOtherGraph;
import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.Counters;
import swiss.sib.swissprot.voidcounter.QueryCallable;

public class FindPredicateLinkSets extends QueryCallable<Exception> {
	public static final Logger log = LoggerFactory.getLogger(FindPredicateLinkSets.class);
	private final String rawQuery;
	private final Set<ClassPartition> classes;
	private final PredicatePartition pp;
	private final ClassPartition source;
	private final Function<QueryCallable<?>, CompletableFuture<Exception>> schedule;
	
	private PredicatePartition subpredicatePartition;

	
	private final String classExclusion;
	private final CommonVariables cv;
	private final Counters counters;
	private final OptimizeFor optimizeFor;

	public FindPredicateLinkSets(CommonVariables cv, Set<ClassPartition> classes, PredicatePartition predicate,
			ClassPartition source, Function<QueryCallable<?>, CompletableFuture<Exception>> schedule,
			String classExclusion, Counters counters, OptimizeFor optimizeFor) {
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;
		this.classes = classes;
		this.pp = predicate;
		this.source = source;
		this.schedule = schedule;
		this.classExclusion = classExclusion;
		this.counters = counters;
		this.optimizeFor = optimizeFor;
		this.rawQuery = Helper.loadSparqlQuery("count_subjects_with_a_type_and_predicate", optimizeFor);
	}

	private void findDatatypeOrSubclassPartitions(Set<ClassPartition> targetClasses,
			ClassPartition source, PredicatePartition subpredicatePartition) {
		if (subpredicatePartition.getDataTypePartitions().isEmpty()) {
			findSubClassParititions(targetClasses, subpredicatePartition, source);
		}
	}

	private void findSubClassParititions(Set<ClassPartition> targetClasses, PredicatePartition predicatePartition,
			ClassPartition source) {

		schedule.apply(counters.findNamedIndividualObjectSubjectForPredicateInGraph(cv, predicatePartition, source));

		if (optimizeFor.preferGroupBy()) {
			QueryCallable<List<LinkSetToOtherGraph>> linkOrSubClassPartition = counters
					.isSourceClassLinkedToDistinctClassInGraphs(cv, predicatePartition, source, schedule,
							classExclusion);
			schedule.apply(linkOrSubClassPartition);
		} else {
			for (ClassPartition target : targetClasses) {
				schedule.apply(counters.isSourceClassLinkedToTargetClass(cv,target,
						predicatePartition, source));
			}
			for (GraphDescription og : cv.sd().getGraphs()) {
				if (!og.getGraphName().equals(cv.gd().getGraphName())) {
					schedule.apply(
							counters.isSourceClassLinkedToDistinctClassInOtherGraph(cv,predicatePartition,
									source, og, schedule, classExclusion));
				}
			}
		}

		schedule.apply(counters.findDataTypeIfNoClassOrDtKnown(cv, predicatePartition, source));
	}

	private long countTriplesInPredicateClassPartition(final RepositoryConnection connection,
			PredicatePartition predicatePartition, ClassPartition source) {

		try {
			MapBindingSet bs = new MapBindingSet();
			bs.setBinding("graph", cv.gd().getGraph());
			bs.setBinding("sourceClass", source.getClazz());
			bs.setBinding("predicate", predicatePartition.getPredicate());
			setQuery(rawQuery, bs);
			try (TupleQueryResult triples = Helper.runTupleQuery(getQuery(), connection)) {
				if (triples.hasNext()) {
					return ((Literal) triples.next().getBinding("count").getValue()).longValue();
				}
			}
		} catch (MalformedQueryException | QueryEvaluationException e) {
			log.error("query failed", e);
		}
		return 0;
	}

	@Override
	protected void logStart() {
		log.debug(
				"Finding predicate linksets " + cv.gd().getGraphName() + ':' + source.getClazz() + ':' + pp.getPredicate());

	}

	@Override
	protected void logEnd() {
		log.debug("Found predicate linksets " + cv.gd().getGraphName() + ':' + source.getClazz() + ':' + pp.getPredicate());
	}

	@Override
	protected Exception run(RepositoryConnection connection) throws Exception {

		try {
			subpredicatePartition = new PredicatePartition(pp.getPredicate());
			long tripleCount = countTriplesInPredicateClassPartition(connection, pp, source);
			subpredicatePartition.setTripleCount(tripleCount);

		} catch (RepositoryException e) {
			log.error("Finding class and predicate link sets failed", e);
			return e;
		}
		return null;
	}

	@Override
	protected void set(Exception t) {
		if (subpredicatePartition.getTripleCount() > 0) {
			try {
				cv.writeLock().lock();
				source.putPredicatePartition(subpredicatePartition);
			} finally {
				cv.writeLock().unlock();
			}
			if (subpredicatePartition.getTripleCount() != 0) {
				findDatatypeOrSubclassPartitions(classes, source, subpredicatePartition);
			}
			cv.save();
		}
	}
	
	@Override
	protected Logger getLog() {
		return log;
	}
}