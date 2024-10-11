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
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.Binding;
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
import swiss.sib.swissprot.servicedescription.ServiceDescription;
import swiss.sib.swissprot.servicedescription.sparql.Helper;

public final class FindDistinctClassses extends QueryCallable<List<ClassPartition>> {
	private final GraphDescription gd;
	private static final Logger log = LoggerFactory.getLogger(FindDistinctClassses.class);
	private final Lock writeLock;
	private final AtomicInteger finishedQueries;
	private final Consumer<ServiceDescription> saver;
	private final ServiceDescription sd;
	private final Function<QueryCallable<?>, CompletableFuture<Exception>> scheduler;
	private final String classExclusion;
	private final Supplier<QueryCallable<?>> onSuccess;

	public FindDistinctClassses(GraphDescription gd, Repository repository, Lock writeLock, Semaphore limiter,
			AtomicInteger finishedQueries, Consumer<ServiceDescription> saver, Function<QueryCallable<?>, CompletableFuture<Exception>> scheduler,
			ServiceDescription sd, String classExclusion, Supplier<QueryCallable<?>> onSuccess) {
		super(repository, limiter, finishedQueries);
		this.gd = gd;
		this.writeLock = writeLock;
		this.finishedQueries = finishedQueries;
		this.saver = saver;
		this.scheduler = scheduler;
		this.sd = sd;
		this.classExclusion = classExclusion;
		this.onSuccess = onSuccess;
	}

	@Override
	protected void logStart() {
		log.debug("Find distinct classes for " + gd.getGraphName());
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("failed counting distinct classses " + gd.getGraphName(), e);
	}

	@Override
	protected void logEnd() {
		log.debug("Found distinct classes:" + gd.getDistinctClassesCount() + " for " + gd.getGraphName());
	}

	@Override
	protected List<ClassPartition> run(RepositoryConnection connection)
			throws MalformedQueryException, QueryEvaluationException, RepositoryException {
		List<ClassPartition> classesList = new ArrayList<>();
		query = makeQuery();
		try (TupleQueryResult classes = Helper.runTupleQuery(query, connection)) {
			while (classes.hasNext()) {
				Binding classesCount = classes.next().getBinding("clazz");
				Value value = classesCount.getValue();
				if (value.isIRI()) {
					final IRI clazz = (IRI) value;
					classesList.add(new ClassPartition(clazz));
				}
			}
		}
		for (ClassPartition cp : classesList) {
			scheduler.apply(new CountMembersOfClassPartition(repository, limiter, cp, finishedQueries));
		}
		if (onSuccess != null) {
			scheduler.apply(onSuccess.get());
		}
		saver.accept(sd);
		return classesList;
	}

	private String makeQuery() {
		if (classExclusion == null) {
			return "SELECT DISTINCT ?clazz WHERE { GRAPH <" + gd.getGraphName() + "> {?thing a ?clazz }}";
		} else {
			return "SELECT DISTINCT ?clazz WHERE { GRAPH <" + gd.getGraphName() + "> {?thing a ?clazz . FILTER ("
					+ classExclusion + ")}}";
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

	}

	private final class CountMembersOfClassPartition extends QueryCallable<Long> {
		public CountMembersOfClassPartition(Repository repository, Semaphore limiter, ClassPartition cp, AtomicInteger finishedQueries) {
			super(repository, limiter, finishedQueries);
			this.cp = cp;
		}

		private final ClassPartition cp;

		@Override
		protected void logStart() {
			log.debug("Counting distinct triples for clas "+cp.getClazz()+" for " + gd.getGraphName());

		}

		@Override
		protected void logEnd() {
			log.debug("Counted distinct triples for clas "+cp.getClazz()+" for " + gd.getGraphName());
		}

		@Override
		protected Long run(RepositoryConnection connection) throws Exception {
			query = "SELECT (COUNT(?thing) AS ?count) WHERE {GRAPH <" + gd.getGraphName() + "> {?thing a <"
					+ cp.getClazz().stringValue() + "> }}";
			return Helper.getSingleLongFromSparql(query, connection, "count");
		}

		@Override
		protected void set(Long t) {
			cp.setTripleCount(t);
		}

	}
}