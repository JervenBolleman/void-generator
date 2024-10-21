package swiss.sib.swissprot.voidcounter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.ServiceDescription;

public class FindPredicatesTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(FindPredicatesTest.class);

	@TempDir
	public File folder;

	private Repository repository;

	@BeforeEach
	void setup() throws IOException {
		LOGGER.debug("running setup");
		repository = new SailRepository(new MemoryStore());
		repository.init();
	}

	@AfterEach
	void shutdown() {
		LOGGER.debug("running shutdown");
		repository.shutDown();
		LOGGER.debug("ran shutdown");
	}

	@Test
	void empty() throws IOException {

		final ServiceDescription sd = new ServiceDescription();
		final GraphDescription gd = new GraphDescription();
		gd.setGraphName("https://sparql.uniprot.org/uniprot");
		sd.putGraphDescription(gd);
		Lock writeLock = new ReentrantLock();
		AtomicInteger finishedQueries = new AtomicInteger(0);
		final FindPredicates countDistinctIriSubjectsForAllGraphs = new FindPredicates(gd, repository, Set.of(),
				(s)->null, writeLock, new Semaphore(1),
				finishedQueries, (isd) -> {
				}, sd, null);
		countDistinctIriSubjectsForAllGraphs.call();
		assertEquals(0, gd.getPredicates().size());
		assertEquals(1, finishedQueries.get());
	}

	@Test
	void one() throws IOException {

		try (RepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			final SimpleValueFactory svf = SimpleValueFactory.getInstance();
			Statement stat = svf.createStatement(RDF.FIRST, RDF.FIRST, RDF.FIRST);
			connection.add(stat, RDF.BAG);
			connection.commit();
		}
		final ServiceDescription sd = new ServiceDescription();
		final GraphDescription gd = new GraphDescription();
		gd.setGraph(RDF.BAG);
		sd.putGraphDescription(gd);
		Lock writeLock = new ReentrantLock();
		AtomicInteger finishedQueries = new AtomicInteger(0);

		final FindPredicates countDistinctIriSubjectsForAllGraphs = new FindPredicates(gd, repository, Set.of(),
				(s)->null, writeLock, new Semaphore(1),
				finishedQueries, (isd) -> {
				}, sd, null);
		countDistinctIriSubjectsForAllGraphs.call();
		assertEquals(1, gd.getPredicates().size());
		assertEquals(1, finishedQueries.get());
	}

	@Test
	void oneLast() throws IOException {

		try (RepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			final SimpleValueFactory svf = SimpleValueFactory.getInstance();
			Statement stat = svf.createStatement(RDF.FIRST, RDF.FIRST, RDF.FIRST);
			connection.add(stat, RDF.BAG);
			connection.commit();
		}
		final ServiceDescription sd = new ServiceDescription();
		final GraphDescription gd = new GraphDescription();
		gd.setGraph(RDF.BAG);
		sd.putGraphDescription(gd);
		Lock writeLock = new ReentrantLock();
		AtomicInteger finishedQueries = new AtomicInteger(0);
		final FindPredicates countDistinctIriSubjectsForAllGraphs = new FindPredicates(gd, repository, Set.of(),
				(s)->null, writeLock, new Semaphore(1),
				finishedQueries, (isd) -> {
				}, sd, null);
		countDistinctIriSubjectsForAllGraphs.call();
		assertEquals(1, gd.getPredicates().size());
		assertEquals(1, finishedQueries.get());
	}
}
