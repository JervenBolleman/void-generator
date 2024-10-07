package swiss.sib.swissprot.voidcounter.virtuoso;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.ServiceDescription;
import swiss.sib.swissprot.voidcounter.QueryCallable;
import virtuoso.rdf4j.driver.VirtuosoRepositoryConnection;

public final class CountDistinctIriSubjectsAndObjectsInAGraphVirtuoso extends
		QueryCallable<swiss.sib.swissprot.voidcounter.virtuoso.CountDistinctIriSubjectsAndObjectsInAGraphVirtuoso.SubObj> {

	private static final int MAX_IN_PROCESS = 32;
	private final Semaphore inProcess = new Semaphore(MAX_IN_PROCESS);
	private static final ExecutorService ES = Executors.newCachedThreadPool();
	private static final int RUN_OPTIMIZE_EVERY = 128;
	private static final int COMMIT_TO_RB_AT = 16 * 4096;
	private static final Logger log = LoggerFactory.getLogger(CountDistinctIriSubjectsAndObjectsInAGraphVirtuoso.class);
	protected final ServiceDescription sd;
	protected final Consumer<ServiceDescription> saver;
	protected final String graphIri;
	private final Lock writeLock;
	private final Consumer<Long> objectAllSetter;
	private final Consumer<Long> subjectAllSetter;
	private final BiConsumer<GraphDescription, Long> graphObjectSetter;
	private final BiConsumer<GraphDescription, Long> graphSubjectSetter;
	private final Map<String, Roaring64NavigableMap> objectGraphIriIds;
	private final Map<String, Roaring64NavigableMap> subjectGraphIriIds;
	private final ExecutorService shared;

	public CountDistinctIriSubjectsAndObjectsInAGraphVirtuoso(ServiceDescription sd, Repository repository,
			Consumer<ServiceDescription> saver, Lock writeLock, Map<String, Roaring64NavigableMap> graphSubjectIriIds,
			Map<String, Roaring64NavigableMap> graphObjectIriIds, String graphIri, Semaphore limit, ExecutorService shared) {
		super(repository, limit);
		this.subjectGraphIriIds = graphSubjectIriIds;
		this.objectGraphIriIds = graphObjectIriIds;
		this.shared = shared;
		this.objectAllSetter = sd::setDistinctIriObjectCount;
		this.subjectAllSetter = sd::setDistinctIriSubjectCount;
		this.graphObjectSetter = GraphDescription::setDistinctIriObjectCount;
		this.graphSubjectSetter = GraphDescription::setDistinctIriSubjectCount;
		this.writeLock = writeLock;
		this.sd = sd;
		this.saver = saver;
		this.graphIri = graphIri;
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct iri objects and subjects for " + graphIri);
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("failed counting distinct iri objects and subjects" + graphIri, e);
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct iri objects and subjects for graph " + graphIri);
	}

	protected String queryForGraph() {
		return """
				select
				case
				when is_named_iri_id(RDF_QUAD.S) then iri_id_num(RDF_QUAD.S)
				else -1
				end as sid,
				case
				when is_named_iri_id(RDF_QUAD.O) then iri_id_num(RDF_QUAD.O)
				else -1
				end as oid
				from
				RDF_QUAD
				where
				RDF_QUAD.G = iri_to_id('""" + graphIri + "')";
	}

	protected SubObj findUniqueIriIds(final Connection quadStoreConnection) {
		Roaring64NavigableMap subjects = new Roaring64NavigableMap();
		Roaring64NavigableMap objects = new Roaring64NavigableMap();
		try (final Statement createStatement = quadStoreConnection.createStatement()) {
			createStatement.setFetchDirection(ResultSet.FETCH_FORWARD);
			createStatement.setFetchSize(4 * 1024);
			extractUniqueIRIIdsPerGraph(createStatement, subjects, objects);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return new SubObj(subjects, objects);
	}

	public static record SubObj(Roaring64NavigableMap subjects, Roaring64NavigableMap objects) {

	}

	@Override
	protected SubObj run(RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		assert connection instanceof VirtuosoRepositoryConnection;
		// See http://docs.openlinksw.com/virtuoso/rdfiriidtype/
		final Connection quadStoreConnection = ((VirtuosoRepositoryConnection) connection).getQuadStoreConnection();

		// If we did not save this data before recalculate it.
//		
		SubObj so;
		if (!objectGraphIriIds.containsKey(graphIri) || !subjectGraphIriIds.containsKey(graphIri)) {

			so = findUniqueIriIds(quadStoreConnection);
			subjectGraphIriIds.put(graphIri, so.subjects);
			objectGraphIriIds.put(graphIri, so.objects);
		} else {
			Roaring64NavigableMap subjects = subjectGraphIriIds.get(graphIri);
			Roaring64NavigableMap objects = objectGraphIriIds.get(graphIri);
			so = new SubObj(subjects, objects);
		}
		setAll();
		return so;
	}

	protected void extractUniqueIRIIdsPerGraph(final Statement createStatement, Roaring64NavigableMap subjects,
			Roaring64NavigableMap objects) throws SQLException {

		String query = queryForGraph();
		final ReentrantLock subLock = new ReentrantLock();
		final ReentrantLock objLock = new ReentrantLock();
		AddedStatus subjectIdx = new AddedStatus(0, 0, new Roaring64NavigableMap(), subLock);
		AddedStatus objectIdx = new AddedStatus(0, 0, new Roaring64NavigableMap(), objLock);
		try (ResultSet rs = createStatement.executeQuery(query)) {
			while (rs.next()) {
				subjectIdx = add(subjects, subjectIdx, rs.getLong(1));
				objectIdx = add(objects, objectIdx, rs.getLong(2));
			}
		}
		try {
			subLock.lock();
			subjects.or(subjectIdx.temp);
		} finally {
			subLock.unlock();
		}
		try {
			objLock.lock();
			objects.or(objectIdx.temp);
		} finally {
			objLock.unlock();
		}
		ES.shutdown();
		while (!ES.isTerminated()) {
			try {
				ES.awaitTermination(100, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				Thread.interrupted();
			}
		}
		inProcess.acquireUninterruptibly(MAX_IN_PROCESS);
	}

	/**
	 * Used because the getLongCardinality method is very expensive on a
	 * Roaring64Bitmap
	 */
	private record AddedStatus(long addedToTemp, long runOptimizeCounter, Roaring64NavigableMap temp, Lock guard) {
	};

	private AddedStatus add(Roaring64NavigableMap rb, AddedStatus as, long current) {
		if (current >= 0) {
			as.temp.addLong(current);
			if (as.addedToTemp >= COMMIT_TO_RB_AT) {
				return mergeAndOptimizeIfNeeded(rb, as);
			} else {
				return new AddedStatus(as.addedToTemp + 1, as.runOptimizeCounter,as.temp, as.guard);
			}
		}
		return as;
	}

	private AddedStatus mergeAndOptimizeIfNeeded(Roaring64NavigableMap rb, AddedStatus as) {
		enQueue((s) -> new MergeAction(as, rb, s));
		if (as.runOptimizeCounter == RUN_OPTIMIZE_EVERY) {
			enQueue((s) -> new RunOptimizeAction(as, rb, s));
			return new AddedStatus(0, 0, new Roaring64NavigableMap(), as.guard);
		} else {
			return new AddedStatus(0, as.runOptimizeCounter + 1, new Roaring64NavigableMap(), as.guard);
		}
	}

	private abstract class GuardedAction {
		protected final AddedStatus as;
		protected final Roaring64NavigableMap rb;
		/**
		 * May be null, in case we are running on the main thread.
		 */
		private final Semaphore limiter;
		
		public GuardedAction(AddedStatus as, Roaring64NavigableMap rb, Semaphore limiter) {
			super();
			this.as = as;
			this.rb = rb;
			this.limiter = limiter;
		}
		
		protected final void act() {
			try {
				as.guard().lock();
				doAction();
			} finally {
				as.guard().unlock();
				if (limiter != null) {
					limiter.release();
				}
			}
		}
		protected abstract void doAction();
		
	}
	
	private class MergeAction extends GuardedAction {
		public MergeAction(AddedStatus as, Roaring64NavigableMap rb, Semaphore limiter) {
			super(as, rb, limiter);
		}

		protected void doAction() {
			as.temp.runOptimize();
			rb.or(as.temp);
		}
	}
	
	private class RunOptimizeAction extends GuardedAction {
		public RunOptimizeAction(AddedStatus as, Roaring64NavigableMap rb, Semaphore limiter) {
			super(as, rb, limiter);
		}

		protected void doAction() {
			rb.runOptimize();
			rb.or(as.temp);
		}
	}

	/**
	 * We try to run this in the shared queue if there is space otherwise we use the internal action.
	 * If there is no space there then we run it immediately in the thread 
	 * that is running the current code.
	 * @param actionMaker
	 */
	private void enQueue(final Function<Semaphore, GuardedAction> actionMaker) {
		if (limiter.tryAcquire()) {
			GuardedAction apply = actionMaker.apply(limiter);
			shared.submit(apply::act);
		} else if (inProcess.tryAcquire()){
			GuardedAction apply = actionMaker.apply(inProcess);
			ES.submit(apply::act);
		} else {
			GuardedAction apply = actionMaker.apply(null);
			ES.submit(apply::act);
		}
		
	}

	@Override
	protected void set(SubObj count) {
		try {
			writeLock.lock();

			final GraphDescription graph = sd.getGraph(graphIri);
			if (graph != null) {
				graphSubjectSetter.accept(graph, count.subjects.getLongCardinality());
				graphObjectSetter.accept(graph, count.objects.getLongCardinality());
			}
		} finally {
			writeLock.unlock();
		}
		saver.accept(sd);
	}

	protected void setAll() {
		try {

			writeLock.lock();
			subjectAllSetter.accept(countAll(subjectGraphIriIds));
			objectAllSetter.accept(countAll(objectGraphIriIds));
		} finally {
			writeLock.unlock();
		}
	}

	private long countAll(Map<String, Roaring64NavigableMap> objectGraphIriIds2) {
		Roaring64NavigableMap all = new Roaring64NavigableMap();
		for (Roaring64NavigableMap rbm : objectGraphIriIds2.values()) {
			all.or(rbm);
		}
		final long iricounts = all.getLongCardinality();
		return iricounts;
	}
}
