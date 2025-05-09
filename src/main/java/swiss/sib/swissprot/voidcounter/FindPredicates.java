package swiss.sib.swissprot.voidcounter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.PredicatePartition;

public final class FindPredicates extends QueryCallable<List<PredicatePartition>> {
	
	private final Map<IRI, IRI> knownPredicates;
	private static final Logger log = LoggerFactory.getLogger(FindPredicates.class);
	
	private final Function<QueryCallable<?>, CompletableFuture<Exception>> schedule;
	private final Supplier<QueryCallable<?>> onSuccess;
	private static final String QUERY = """
			SELECT ?predicate (COUNT(?object) AS ?count) 
			WHERE { 
				GRAPH ?graph { 
					?subject ?predicate ?object 
				}
			} GROUP BY ?predicate
			""";
	private final CommonVariables cv;
	
	public FindPredicates(CommonVariables cv,Set<IRI> knownPredicates,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, Supplier<QueryCallable<?>> onSuccess) {
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;
		
		this.knownPredicates = new HashMap<>();
		knownPredicates.stream().forEach(p -> this.knownPredicates.put(p, p));
		this.schedule = schedule;

		
		this.onSuccess = onSuccess;
	}

	private List<PredicatePartition> findPredicates(GraphDescription gd, RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		TupleQuery tq = connection.prepareTupleQuery(QUERY);
		tq.setBinding("graph", gd.getGraph());
		setQuery(QUERY, tq.getBindings());
		try (TupleQueryResult predicateQuery = tq.evaluate()) {
			List<PredicatePartition> res = new ArrayList<>();
			while (predicateQuery.hasNext()) {

				BindingSet next = predicateQuery.next();
				Binding predicate = next.getBinding("predicate");
				Binding predicateCount = next.getBinding("count");
				if (predicateCount != null && predicate != null) {
					IRI valueOf = (IRI) predicate.getValue();
					PredicatePartition pp;
					//Normalize in memory
					if (knownPredicates.containsKey(valueOf)) {
						pp = new PredicatePartition(knownPredicates.get(valueOf));
					} else {
						pp = new PredicatePartition(valueOf);
					}
					final long count = ((Literal) predicateCount.getValue()).longValue();
					if (count > 0 ) {
						pp.setTripleCount(count);
						res.add(pp);
					}
				}
			}
			return res;
		} finally {
			cv.save();
			if (onSuccess != null) {
				schedule.apply(onSuccess.get());
			}
		}
	}

	@Override
	protected void logStart() {
		log.debug("Finding distinct predicates for {}",  cv.gd().getGraphName());
	}

	@Override
	protected void logEnd() {
		log.debug("Found distinct predicates:{} for {}",  cv.gd().getPredicates().size() ,cv.gd().getGraphName());
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
		for (PredicatePartition predicatePartition : predicates) {
			schedule.apply(new CountUniqueSubjectPerPredicateInGraph(cv, predicatePartition));
			schedule.apply(new CountUniqueObjectsPerPredicateInGraph(cv, predicatePartition));
		}
	}
	
	@Override
	protected Logger getLog() {
		return log;
	}
}