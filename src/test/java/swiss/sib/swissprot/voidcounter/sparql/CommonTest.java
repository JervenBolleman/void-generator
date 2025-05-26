package swiss.sib.swissprot.voidcounter.sparql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
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
	protected List<CompletableFuture<Exception>> futures;
	protected Consumer<ServiceDescription> saver = (s) -> {
		// Default no-op saver
	};
	
	protected Function<QueryCallable<?, ? extends Variables>, CompletableFuture<Exception>> schedule = (isd) -> {
		var e = isd.call();
		CompletableFuture<Exception> completedFuture = CompletableFuture.completedFuture(e);
		futures.add(completedFuture);
		return completedFuture;
	};

	GraphDescription emptyGd;

	@BeforeEach
	void setup() throws IOException {
		repository = new SailRepository(new MemoryStore());
		emptyGd = new GraphDescription();
		emptyGd.setGraphName("https://example.org/empty");
		futures = new ArrayList<>();
	}

	@AfterEach
	void shutdown() {
		repository.shutDown();
		futures = null;
	}

	protected CommonVariables createCommonVariables() {

		final ServiceDescription sd = new ServiceDescription();
		AtomicInteger finishedQueries = new AtomicInteger(0);
		CommonVariables cv = new CommonVariables(sd, repository, saver, new ReentrantReadWriteLock(), new Semaphore(1), finishedQueries);
		return cv;
	}
}
