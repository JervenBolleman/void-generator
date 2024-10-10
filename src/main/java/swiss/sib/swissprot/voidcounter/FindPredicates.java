package swiss.sib.swissprot.voidcounter;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.function.Function;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.servicedescription.ServiceDescription;
import swiss.sib.swissprot.servicedescription.sparql.Helper;

public final class FindPredicates extends QueryCallable<List<PredicatePartition>> {
	private final GraphDescription gd;
	private final Set<IRI> knownPredicates;
	private static final Logger log = LoggerFactory.getLogger(FindPredicates.class);
	private final Lock writeLock;
	private final AtomicInteger finishedQueries;
	private final Consumer<ServiceDescription> saver;
	private final ServiceDescription sd;
	private final Function<QueryCallable<?>, CompletableFuture<Exception>> schedule;

	public FindPredicates(GraphDescription gd, Repository repository, Set<IRI> knownPredicates,
			Function<QueryCallable<?>, CompletableFuture<Exception>> schedule, Lock writeLock, Semaphore limiter, AtomicInteger finishedQueries, Consumer<ServiceDescription> saver, ServiceDescription sd) {
		super(repository, limiter);
		this.gd = gd;
		this.knownPredicates = knownPredicates;
		this.schedule = schedule;

		this.writeLock = writeLock;
		this.saver = saver;
		this.sd = sd;
		this.finishedQueries = finishedQueries;
	}

	private List<PredicatePartition> findPredicates(GraphDescription gd, RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		query = "SELECT ?predicate (COUNT(?object) AS ?count) WHERE { GRAPH <" + gd.getGraphName()
						+ "> { ?subject ?predicate ?object }} GROUP BY ?predicate";
		try (TupleQueryResult predicateQuery = Helper
				.runTupleQuery(query, connection)) {
			List<PredicatePartition> res = new ArrayList<>();
			while (predicateQuery.hasNext()) {

				BindingSet next = predicateQuery.next();
				Binding predicate = next.getBinding("predicate");
				Binding predicateCount = next.getBinding("count");
				if (predicateCount != null && predicate != null) {
					IRI valueOf = (IRI) predicate.getValue();
					PredicatePartition pp = new PredicatePartition(valueOf);
					final long count = ((Literal) predicateCount.getValue()).longValue();
					if (count > 0 ) {
						pp.setTripleCount(count);
						res.add(pp);
					}
				}
			}
			return res;
		} finally {
			finishedQueries.incrementAndGet();
			saver.accept(sd);
		}
	}

	@Override
	protected void logStart() {
		log.debug("Finding distinct predicates for " + gd.getGraphName());
	}

	@Override
	protected void logEnd() {
		log.debug("Found distinct predicates:" + gd.getPredicates().size() + " for " + gd.getGraphName());
	}

	@Override
	protected List<PredicatePartition> run(RepositoryConnection connection) throws Exception {
		return findPredicates(gd, connection);
	}

	@Override
	protected void set(List<PredicatePartition> predicates) {

		try {
			writeLock.lock();
			Set<PredicatePartition> predicates2 = gd.getPredicates();
			predicates2.clear();
			for (PredicatePartition predicate : predicates)
				if (knownPredicates.contains(predicate.getPredicate())) {
					knownPredicates.stream().filter(p -> p.equals(predicate.getPredicate()))
							.forEach(p -> predicate.setPredicate(p));
					predicates2.add(predicate);
				} else {
					predicates2.add(predicate);
				}
		} finally {
			writeLock.unlock();
		}
		for (PredicatePartition predicatePartition : predicates) {
			schedule.apply(new CountUniqueSubjectPerPredicateInGraph(gd, predicatePartition, repository, writeLock, limiter));
			schedule.apply(new CountUniqueObjectsPerPredicateInGraph(gd, predicatePartition, repository, writeLock, limiter));
		}
	}
}