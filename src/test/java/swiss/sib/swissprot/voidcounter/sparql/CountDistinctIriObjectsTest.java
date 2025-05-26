package swiss.sib.swissprot.voidcounter.sparql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.ServiceDescription;
import swiss.sib.swissprot.voidcounter.CommonVariables;

class CountDistinctIriObjectsTest {
	private Repository repository;

	@BeforeEach
	void setup() throws IOException {

		repository = new SailRepository(new MemoryStore());
	}

	@AfterEach
	void shutdown() {
		repository.shutDown();
	}

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void empty(OptimizeFor of) throws IOException {

		final ServiceDescription sd = new ServiceDescription();
		AtomicInteger finishedQueries = new AtomicInteger(0);
		CommonVariables cv = new CommonVariables(sd, repository, (s) -> {
		}, new ReentrantReadWriteLock(), new Semaphore(1), finishedQueries);

		var count = new CountDistinctIriObjectsInDefaultGraph(cv, of);
		count.call();
		assertEquals(0, sd.getDistinctIriObjectCount());
		assertEquals(1, finishedQueries.get());

		var gd = new GraphDescription();
		gd.setGraph(RDF.BAG);
		var gcv = cv.with(gd);
		var count2 = new CountDistinctIriObjectsInAGraph(gcv, OptimizeFor.SPARQL);
		count2.call();
		assertEquals(0, gd.getDistinctIriObjectCount());
		assertEquals(2, finishedQueries.get());
	}

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void one(OptimizeFor of) throws IOException {

		try (RepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			final SimpleValueFactory svf = SimpleValueFactory.getInstance();
			Statement stat = svf.createStatement(RDF.FIRST, RDF.FIRST, RDF.FIRST, RDF.BAG);
			connection.add(stat, RDF.BAG);
			connection.commit();
		}
		final ServiceDescription sd = new ServiceDescription();
		AtomicInteger finishedQueries = new AtomicInteger(0);
		CommonVariables cv = new CommonVariables(sd, repository, (s) -> {
		}, new ReentrantReadWriteLock(), new Semaphore(1), finishedQueries);

		var countDistinctIriObjectsForAllGraphs = new CountDistinctIriObjectsInDefaultGraph(cv, of);
		countDistinctIriObjectsForAllGraphs.call();
		assertEquals(1, sd.getDistinctIriObjectCount());
		assertEquals(1, finishedQueries.get());

		var gd = new GraphDescription();
		gd.setGraph(RDF.BAG);
		sd.putGraphDescription(gd);
		var gcv = cv.with(gd);
		var count2 = new CountDistinctIriObjectsInAGraph(gcv, OptimizeFor.SPARQL);
		count2.call();
		assertEquals(1, gd.getDistinctIriObjectCount());
		assertEquals(2, finishedQueries.get());
	}
}
