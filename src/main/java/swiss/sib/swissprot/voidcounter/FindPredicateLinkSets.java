package swiss.sib.swissprot.voidcounter;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.servicedescription.ServiceDescription;
import swiss.sib.swissprot.servicedescription.sparql.Helper;

public class FindPredicateLinkSets extends QueryCallable<Exception> {
	public static final Logger log = LoggerFactory.getLogger(FindPredicateLinkSets.class);

	private final Set<ClassPartition> classes;
	private final PredicatePartition pp;
	private final ClassPartition source;
	private final Lock writeLock;
	private final List<Future<Exception>> futures;
	private final ExecutorService execs;
	private final GraphDescription gd;

	private PredicatePartition subpredicatePartition;

	private final AtomicInteger scheduledQueries;

	private final AtomicInteger finishedQueries;

	private final Consumer<ServiceDescription> saver;

	private final ServiceDescription sd;

	public FindPredicateLinkSets(Repository repository, Set<ClassPartition> classes, PredicatePartition predicate,
			ClassPartition source, Lock writeLock, List<Future<Exception>> futures, Semaphore limit,
			ExecutorService execs, GraphDescription gd, AtomicInteger scheduledQueries, AtomicInteger finishedQueries,
			Consumer<ServiceDescription> saver, ServiceDescription sd) {
		super(repository, limit);
		this.classes = classes;
		this.pp = predicate;
		this.source = source;
		this.writeLock = writeLock;
		this.futures = futures;
		this.execs = execs;
		this.gd = gd;
		this.scheduledQueries = scheduledQueries;
		this.finishedQueries = finishedQueries;
		this.saver = saver;
		this.sd = sd;
		scheduledQueries.incrementAndGet();
	}

	private void findDatatypeOrSubclassPartitions(final Repository repository, Set<ClassPartition> targetClasses,
			ClassPartition source, PredicatePartition subpredicatePartition, Lock writeLock) {
		if (subpredicatePartition.getDataTypePartitions().isEmpty()) {
			findSubClassParititions(targetClasses, subpredicatePartition, source, repository, writeLock);
		}
	}

	private void findSubClassParititions(Set<ClassPartition> targetClasses, PredicatePartition predicatePartition,
			ClassPartition source, Repository repository, Lock writeLock) {
		final IRI predicate = predicatePartition.getPredicate();
		futures.add(execs.submit(new FindNamedIndividualObjectSubjectForPredicateInGraph(gd, predicatePartition, source,
				repository, writeLock, limiter, scheduledQueries, finishedQueries)));

		for (ClassPartition target : targetClasses) {

			Future<Exception> future = execs.submit(new IsSourceClassLinkedToTargetClass(repository, predicate, target,
					predicatePartition, source, gd, writeLock, limiter, scheduledQueries, finishedQueries));
			futures.add(future);

		}

		for (GraphDescription og : sd.getGraphs()) {
			if (!og.getGraphName().equals(gd.getGraphName())) {
				Future<Exception> future = execs.submit(
						new IsSourceClassLinkedToDistinctClassInOtherGraph(repository, predicate, predicatePartition,
								source, gd, writeLock, limiter, scheduledQueries, finishedQueries, og, execs, futures));
				futures.add(future);
			}
		}
		futures.add(execs.submit(new FindDataTypeIfNoClassOrDtKnown(predicatePartition, source, repository, gd,
				writeLock, limiter, scheduledQueries, finishedQueries)));
	}

	private long countTriplesInPredicateClassPartition(final Repository repository,
			PredicatePartition predicatePartition, ClassPartition source) {

		try (RepositoryConnection localConnection = repository.getConnection()) {
			final String query = "SELECT (COUNT(?subject) AS ?count) WHERE {GRAPH <" + gd.getGraphName()
					+ "> {?subject a <" + source.getClazz() + "> ; <" + predicatePartition.getPredicate()
					+ "> ?target .}}";
			try (TupleQueryResult triples = Helper.runTupleQuery(query, localConnection)) {
				if (triples.hasNext()) {
					return ((Literal) triples.next().getBinding("count").getValue()).longValue();
				}
			}
		} catch (MalformedQueryException | QueryEvaluationException e) {
			log.error("query failed", e);
		} finally {
			finishedQueries.incrementAndGet();
		}

		return 0;
	}

	@Override
	protected void logStart() {
		log.debug(
				"Finding predicate linksets " + gd.getGraphName() + ':' + source.getClazz() + ':' + pp.getPredicate());

	}

	@Override
	protected void logEnd() {
		log.debug("Found predicate linksets " + gd.getGraphName() + ':' + source.getClazz() + ':' + pp.getPredicate());
	}

	@Override
	protected Exception run(RepositoryConnection connection) throws Exception {

		try {
			subpredicatePartition = new PredicatePartition(pp.getPredicate());
			long tripleCount = countTriplesInPredicateClassPartition(repository, pp, source);
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
				writeLock.lock();
				source.putPredicatePartition(subpredicatePartition);
			} finally {
				writeLock.unlock();
			}
			if (subpredicatePartition.getTripleCount() != 0) {
				findDatatypeOrSubclassPartitions(repository, classes, source, subpredicatePartition, writeLock);
			}
			saver.accept(sd);
		}
	}
}