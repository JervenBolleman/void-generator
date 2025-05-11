package swiss.sib.swissprot.voidcounter.sparql;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
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
import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

public final class FindDistinctClassses extends QueryCallable<List<ClassPartition>> {
	private static final String REPLACE = "###REPLACE###";
	private final String nestedLoopQuery;
	private final String groupByQuery;
	private static final Logger log = LoggerFactory.getLogger(FindDistinctClassses.class);

	private final Function<QueryCallable<?>, CompletableFuture<Exception>> scheduler;
	private final String classExclusion;
	private final Supplier<QueryCallable<?>> onSuccess;
	private final CommonVariables cv;
	private final OptimizeFor optimizeFor;

	public FindDistinctClassses(CommonVariables cv,
			Function<QueryCallable<?>, CompletableFuture<Exception>> scheduler, 
			String classExclusion, Supplier<QueryCallable<?>> onSuccess, OptimizeFor optimizeFor) {
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;
		this.scheduler = scheduler;
		this.classExclusion = classExclusion;
		this.onSuccess = onSuccess;
		this.optimizeFor = optimizeFor;
		this.nestedLoopQuery = Helper.loadSparqlQuery("distinct_types_in_a_graph", optimizeFor);
		this.groupByQuery = Helper.loadSparqlQuery("count_occurences_of_distinct_types_in_a_graph", optimizeFor);
	}

	@Override
	protected void logStart() {
		log.debug("Find distinct classes for {}", cv.gd().getGraphName());
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("failed finding distinct classses " + cv.gd().getGraphName(), e);
	}

	@Override
	protected void logEnd() {
		log.debug("Found distinct classes:{} for {}", cv.gd().getDistinctClassesCount(), cv.gd().getGraphName());
	}

	@Override
	protected List<ClassPartition> run(RepositoryConnection connection)
			throws MalformedQueryException, QueryEvaluationException, RepositoryException {
		List<ClassPartition> classesList = new ArrayList<>();
		MapBindingSet tq = new MapBindingSet();
		tq.setBinding("graph", cv.gd().getGraph());
		if (optimizeFor.preferGroupBy())
			groupBy(connection, classesList, tq);
		else
			nested(connection, classesList, tq);
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
			scheduler.apply(new CountMembersOfClassPartition(repository, limiter, cp, finishedQueries, optimizeFor));
		}
	}

	private String makeNestedQuery() {
		if (classExclusion == null || classExclusion.isBlank()) {
			return nestedLoopQuery;
		} else {
			return nestedLoopQuery.replace(REPLACE, "FILTER (" + classExclusion + ")");
		}
	}
	
	private String makeGroupByQuery() {
		if (classExclusion == null || classExclusion.isBlank()) {
			return groupByQuery;
		} else {
			return groupByQuery.replace(REPLACE, "FILTER (" + classExclusion + ")");
		}
	}

	@Override
	protected void set(List<ClassPartition> count) {
		try {
			cv.writeLock().lock();
			Set<ClassPartition> classes = cv.gd().getClasses();
			classes.clear();
			classes.addAll(count);
		} finally {
			cv.writeLock().unlock();
		}
		cv.save();
	}

	@Override
	protected Logger getLog() {
		return log;
	}

	private final class CountMembersOfClassPartition extends QueryCallable<Long> {
		private final String count_type_arcs;

		public CountMembersOfClassPartition(Repository repository, Semaphore limiter, ClassPartition cp,
				AtomicInteger finishedQueries, OptimizeFor optimizeFor) {
			super(repository, limiter, finishedQueries);
			this.cp = cp;
			this.count_type_arcs = Helper.loadSparqlQuery("count_triples_for_type_in_graph", optimizeFor);
		}

		private final ClassPartition cp;

		@Override
		protected void logStart() {
			log.debug("Counting distinct triples for class {} for {}", cp.getClazz(), cv.gd().getGraphName());
		}

		@Override
		protected void logFailed(Exception e) {
			if (log.isErrorEnabled())
				log.error("failed counting distinct triples for class " + cv.gd().getGraphName(), e);
		}

		@Override
		protected void logEnd() {
			log.debug("Counted distinct triples for class {} for {} ", cp.getClazz(), cv.gd().getGraphName());
		}

		@Override
		protected Long run(RepositoryConnection connection) throws Exception {

			MapBindingSet bs = new MapBindingSet();
			bs.setBinding("graph", cv.gd().getGraph());
			bs.setBinding("class", cp.getClazz());
			setQuery(count_type_arcs, bs);
			return Helper.getSingleLongFromSparql(getQuery(), connection, "count");
		}

		@Override
		protected void set(Long t) {
			try {
				cv.writeLock().lock();
				if (t > 0) {
					cp.setTripleCount(t);
				} else {
					cv.gd().getClasses().remove(cp);
				}
			} finally {
				cv.writeLock().unlock();
			}
		}

		@Override
		protected Logger getLog() {
			return log;
		}

	}
}