package swiss.sib.swissprot.voidcounter.sparql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

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

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.servicedescription.ServiceDescription;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

class IsSourceClassLinkedToTargetClassTest {
	private Repository repository;
	private ExecutorService executors;
	@BeforeEach
	void setup() throws IOException {

		repository = new SailRepository(new MemoryStore());
		executors = Executors.newFixedThreadPool(1);
	}

	@AfterEach
	void shutdown() {
		repository.shutDown();
		executors.shutdown();
	}

	@Test
	void one() throws IOException {

		Function<QueryCallable<?>, CompletableFuture<Exception>> scheduler = (q)->{
			return CompletableFuture.supplyAsync(q::call, executors); 
		};
		try (RepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			final SimpleValueFactory svf = SimpleValueFactory.getInstance();
			Statement stat = svf.createStatement(RDF.FIRST, RDF.TYPE, RDF.ALT, RDF.BAG);
			Statement stat1 = svf.createStatement(RDF.FIRST, RDF.HTML, RDF.OBJECT, RDF.BAG);
			Statement stat2 = svf.createStatement(RDF.OBJECT, RDF.TYPE, RDF.BAG, RDF.LI);
			connection.add(stat, RDF.BAG);
			connection.add(stat1, RDF.BAG);
			connection.add(stat2, RDF.LI);
			connection.commit();
		}
		PredicatePartition pp = new PredicatePartition(RDF.HTML);
		ClassPartition source = new ClassPartition(RDF.ALT);
		var bag = new GraphDescription();
		bag.setGraph(RDF.BAG);
		bag.getClasses().add(source);
		source.putPredicatePartition(pp);
		var li = new GraphDescription();
		li.setGraph(RDF.LI);
		ClassPartition target = new ClassPartition(RDF.BAG);
		li.getClasses().add(target);
		ServiceDescription sd = new ServiceDescription();
		AtomicInteger finishedQueries = new AtomicInteger(0);
		Lock writeLock = new ReentrantLock();
		CommonVariables cv = new CommonVariables(sd , bag, repository, s->{}, writeLock, new Semaphore(1), finishedQueries, false);
		var counter = new IsSourceClassLinkedToDistinctClassInOtherGraph(cv, pp, source, li, scheduler, null);
		counter.call();
		assertEquals(1, pp.getLinkSets().size());
		assertEquals(1, finishedQueries.get());
	}
}
