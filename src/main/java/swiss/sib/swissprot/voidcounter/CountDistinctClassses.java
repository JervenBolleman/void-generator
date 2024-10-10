package swiss.sib.swissprot.voidcounter;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

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

public final class CountDistinctClassses extends QueryCallable<List<ClassPartition>> {
	private final GraphDescription gd;
	private static final Logger log = LoggerFactory.getLogger(CountDistinctClassses.class);
	private final Lock writeLock;
	private final AtomicInteger finishedQueries;
	private final Consumer<ServiceDescription> saver;
	private final ServiceDescription sd;
	private final Consumer<QueryCallable<?>> scheduler;

	public CountDistinctClassses(GraphDescription gd, Repository repository, Lock writeLock, Semaphore limiter,
			AtomicInteger finishedQueries, Consumer<ServiceDescription> saver, Consumer<QueryCallable<?>> scheduler,
			ServiceDescription sd) {
		super(repository, limiter);
		this.gd = gd;
		this.writeLock = writeLock;
		this.finishedQueries = finishedQueries;
		this.saver = saver;
		this.scheduler = scheduler;
		this.sd = sd;
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct classes for " + gd.getGraphName());
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("failed counting distinct classses " + gd.getGraphName(), e);
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct classes:" + gd.getDistinctClassesCount() + " for " + gd.getGraphName());
	}

	@Override
	protected List<ClassPartition> run(RepositoryConnection connection)
			throws MalformedQueryException, QueryEvaluationException, RepositoryException {
		List<ClassPartition> classesList = new ArrayList<>();
		query = "SELECT DISTINCT ?clazz WHERE { GRAPH <" + gd.getGraphName() + "> {?thing a ?clazz }}";
		try (TupleQueryResult classes = Helper.runTupleQuery(query, connection)) {
			while (classes.hasNext()) {
				Binding classesCount = classes.next().getBinding("clazz");
				Value value = classesCount.getValue();
				if (value.isIRI()) {
					final IRI clazz = (IRI) value;
					classesList.add(new ClassPartition(clazz));
				}
			}
		} finally {
			finishedQueries.incrementAndGet();
		}
		for (ClassPartition cp : classesList) {
			scheduler.accept(new CountMembersOfClassPartition(repository, limiter, cp));
		}
		saver.accept(sd);
		return classesList;
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
		public CountMembersOfClassPartition(Repository repository, Semaphore limiter, ClassPartition cp) {
			super(repository, limiter);
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
			try {
				return Helper.getSingleLongFromSparql(query, connection, "count");
			} finally {
				finishedQueries.incrementAndGet();
			}
		}

		@Override
		protected void set(Long t) {
			cp.setTripleCount(t);
		}

	}
}