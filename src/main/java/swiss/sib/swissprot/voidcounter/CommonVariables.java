package swiss.sib.swissprot.voidcounter;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

import org.eclipse.rdf4j.repository.Repository;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.ServiceDescription;

public record CommonVariables(ServiceDescription sd, GraphDescription gd, Repository repository,
		Consumer<ServiceDescription> saver, Lock writeLock, Semaphore limiter, AtomicInteger finishedQueries, boolean preferGroupBy) {

	public void save() {
		saver.accept(sd);
	}
}
