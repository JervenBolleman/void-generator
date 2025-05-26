package swiss.sib.swissprot.voidcounter.sparql;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.ServiceDescription;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;
import swiss.sib.swissprot.voidcounter.Variables;

abstract class CommonTest {
	protected Repository repository;
	protected Function<QueryCallable<?, ? extends Variables>, CompletableFuture<Exception>> schedule = (isd) -> null;

	GraphDescription emptyGd;
	
	
	@BeforeEach
	void setup() throws IOException {
		repository = new SailRepository(new MemoryStore());
		emptyGd = new GraphDescription();
		emptyGd.setGraphName("https://example.org/empty");
	}

	@AfterEach
	void shutdown() {
		repository.shutDown();
	}

	protected CommonVariables createCommonVariables() {

		final ServiceDescription sd = new ServiceDescription();
		AtomicInteger finishedQueries = new AtomicInteger(0);
		CommonVariables cv = new CommonVariables(sd, repository, (s) -> {
		}, new ReentrantReadWriteLock(), new Semaphore(1), finishedQueries);
		return cv;
	}
}
