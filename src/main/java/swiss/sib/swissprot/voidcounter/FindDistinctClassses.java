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
import java.util.function.Supplier;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.ServiceDescription;
import swiss.sib.swissprot.servicedescription.sparql.Helper;

public final class FindDistinctClassses extends QueryCallable<List<ClassPartition>> {
	private static final String REPLACE = "###REPLACE###";
	private static final String NESTED_LOOP_QUERY = Helper.loadSparqlQuery("distinct_types_in_a_graph");
	private static final String GROUP_BY_QUERY = Helper.loadSparqlQuery("count_occurences_of_distinct_types_in_a_graph");
	private static final Logger log = LoggerFactory.getLogger(FindDistinctClassses.class);

	private final GraphDescription gd;
	private final Lock writeLock;
	private final AtomicInteger finishedQueries;
	private final Consumer<ServiceDescription> saver;
	private final ServiceDescription sd;
	private final Function<QueryCallable<?>, CompletableFuture<Exception>> scheduler;
	private final String classExclusion;
	private final Supplier<QueryCallable<?>> onSuccess;
	private final boolean nested;

	public FindDistinctClassses(GraphDescription gd, Repository repository, Lock writeLock, Semaphore limiter,
			AtomicInteger finishedQueries, Consumer<ServiceDescription> saver,
			Function<QueryCallable<?>, CompletableFuture<Exception>> scheduler, ServiceDescription sd,
			String classExclusion, Supplier<QueryCallable<?>> onSuccess, boolean preferGroupBy) {
		super(repository, limiter, finishedQueries);
		this.gd = gd;
		this.writeLock = writeLock;
		this.finishedQueries = finishedQueries;
		this.saver = saver;
		this.scheduler = scheduler;
		this.sd = sd;
		this.classExclusion = classExclusion;
		this.onSuccess = onSuccess;
		this.nested = !preferGroupBy;
	}

	@Override
	protected void logStart() {
		log.debug("Find distinct classes for {}", gd.getGraphName());
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("failed finding distinct classses " + gd.getGraphName(), e);
	}

	@Override
	protected void logEnd() {
		log.debug("Found distinct classes:{} for {}", gd.getDistinctClassesCount(), gd.getGraphName());
	}

	@Override
	protected List<ClassPartition> run(RepositoryConnection connection)
			throws MalformedQueryException, QueryEvaluationException, RepositoryException {
		List<ClassPartition> classesList = new ArrayList<>();
		MapBindingSet tq = new MapBindingSet();
		tq.setBinding("graph", gd.getGraph());
		if (nested)
			nested(connection, classesList, tq);
		else
			groupBy(connection, classesList, tq);
		if (onSuccess != null) {
			scheduler.apply(onSuccess.get());
		}
		return classesList;
	}

	private void groupBy(RepositoryConnection connection, List<ClassPartition> classesList, MapBindingSet tq) {
		String rq = makeGroupByQuery();
		setQuery(rq, tq);
		try (TupleQueryResult classes = Helper.runTupleQuery(getQuery(), connection)) {
			while (classes.hasNext()) {
				BindingSet next = classes.next();
				
				Value classesCount = next.getBinding("subjects").getValue();
				Value value = next.getBinding("clazz").getValue();
				//Could be a blank node which we ignore
				if (value instanceof IRI clazz && classesCount instanceof Literal count) {
					ClassPartition cp = new ClassPartition(clazz);
					cp.setTripleCount(count.longValue());
					classesList.add(cp);
				}
			}
		}
	}

	private void nested(RepositoryConnection connection, List<ClassPartition> classesList, MapBindingSet tq) {
		String rq = makeNestedQuery();
		setQuery(rq, tq);
		try (TupleQueryResult classes = Helper.runTupleQuery(getQuery(), connection)) {
			while (classes.hasNext()) {
				BindingSet next = classes.next();
				Value value = next.getBinding("clazz").getValue();
				//Could be a blank node which we ignore
				if (value instanceof IRI clazz) {
					classesList.add(new ClassPartition(clazz));
				}
			}
		}
		for (ClassPartition cp : classesList) {
			scheduler.apply(new CountMembersOfClassPartition(repository, limiter, cp, finishedQueries));
		}
	}

	private String makeNestedQuery() {
		if (classExclusion == null || classExclusion.isBlank()) {
			return NESTED_LOOP_QUERY;
		} else {
			return NESTED_LOOP_QUERY.replaceAll(REPLACE, "FILTER (" + classExclusion + ")");
		}
	}
	
	private String makeGroupByQuery() {
		if (classExclusion == null || classExclusion.isBlank()) {
			return GROUP_BY_QUERY;
		} else {
			return GROUP_BY_QUERY.replaceAll(REPLACE, "FILTER (" + classExclusion + ")");
		}
	}

	@Override
	protected void set(List<ClassPartition> count) {
		try {
			writeLock.lock();
			Set<ClassPartition> classes = gd.getClasses();
			classes.clear();
			classes.addAll(count);
		} finally {
			writeLock.unlock();
		}
		saver.accept(sd);
	}

	@Override
	protected Logger getLog() {
		return log;
	}

	private final class CountMembersOfClassPartition extends QueryCallable<Long> {
		private static final String COUNT_TYPE_ARCS = "SELECT (COUNT(?thing) AS ?count) WHERE {GRAPH ?graph {?thing a ?class }}";

		public CountMembersOfClassPartition(Repository repository, Semaphore limiter, ClassPartition cp,
				AtomicInteger finishedQueries) {
			super(repository, limiter, finishedQueries);
			this.cp = cp;
		}

		private final ClassPartition cp;

		@Override
		protected void logStart() {
			log.debug("Counting distinct triples for class " + cp.getClazz() + " for " + gd.getGraphName());

		}

		@Override
		protected void logFailed(Exception e) {
			log.error("failed counting distinct triples for class " + gd.getGraphName(), e);
		}

		@Override
		protected void logEnd() {
			log.debug("Counted distinct triples for class " + cp.getClazz() + " for " + gd.getGraphName());
		}

		@Override
		protected Long run(RepositoryConnection connection) throws Exception {

			MapBindingSet tq = new MapBindingSet();
			tq.setBinding("graph", gd.getGraph());
			tq.setBinding("class", cp.getClazz());
			setQuery(COUNT_TYPE_ARCS, tq);
			return Helper.getSingleLongFromSparql(getQuery(), connection, "count");
		}

		@Override
		protected void set(Long t) {

			if (t > 0) {
				cp.setTripleCount(t);
			} else {
				gd.getClasses().remove(cp);
			}
		}

		@Override
		protected Logger getLog() {
			return log;
		}

	}
}