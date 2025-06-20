package swiss.sib.swissprot.voidcounter.sparql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.voidcounter.CommonGraphVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

public final class FindPredicatesAndCountObjects extends QueryCallable<List<PredicatePartition>, CommonGraphVariables> {

	private static final Logger log = LoggerFactory.getLogger(FindPredicatesAndCountObjects.class);

	private final Map<IRI, IRI> knownPredicates;
	private final String rawQuery;

	public FindPredicatesAndCountObjects(CommonGraphVariables cv, Set<IRI> knownPredicates,
			OptimizeFor optimizeFor) {
		super(cv);
		this.rawQuery = Helper.loadSparqlQuery("find_predicates_count_objects", optimizeFor);
		this.knownPredicates = new HashMap<>();
		knownPredicates.stream().forEach(p -> this.knownPredicates.put(p, p));
	}

	private List<PredicatePartition> findPredicates(GraphDescription gd, RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		MapBindingSet bs = new MapBindingSet();
		bs.setBinding("graph", gd.getGraph());
		setQuery(rawQuery, bs);
		List<PredicatePartition> res = new ArrayList<>();
		try (TupleQueryResult predicateQuery = Helper.runTupleQuery(getQuery(), connection)) {
			while (predicateQuery.hasNext()) {
				BindingSet next = predicateQuery.next();
				bindingSetToPredicateCount(res, next);
			}
		}
		return res;
	}

	private void bindingSetToPredicateCount(List<PredicatePartition> res, BindingSet next) {
		Binding predicate = next.getBinding("predicate");
		Binding predicateCount = next.getBinding("count");
		if (predicateCount != null && predicate != null) {
			IRI valueOf = (IRI) predicate.getValue();
			PredicatePartition pp;
			// Normalize in memory
			if (knownPredicates.containsKey(valueOf)) {
				pp = new PredicatePartition(knownPredicates.get(valueOf));
			} else {
				pp = new PredicatePartition(valueOf);
			}
			final long count = ((Literal) predicateCount.getValue()).longValue();
			if (count > 0) {
				pp.setTripleCount(count);
				res.add(pp);
			}
		}
	}

	@Override
	protected void logStart() {
		log.debug("Finding distinct predicates for {}", cv.gd().getGraphName());
	}

	@Override
	protected void logEnd() {
		log.debug("Found distinct predicates:{} for {}", cv.gd().getPredicates().size(), cv.gd().getGraphName());
	}

	@Override
	protected List<PredicatePartition> run(RepositoryConnection connection) throws Exception {
		return findPredicates(cv.gd(), connection);
	}

	@Override
	protected void set(List<PredicatePartition> predicates) {

		try {
			cv.writeLock().lock();
			Set<PredicatePartition> predicates2 = cv.gd().getPredicates();
			predicates2.clear();
			predicates2.addAll(predicates);
		} finally {
			cv.writeLock().unlock();
		}
	}

	@Override
	public Logger getLog() {
		return log;
	}
}