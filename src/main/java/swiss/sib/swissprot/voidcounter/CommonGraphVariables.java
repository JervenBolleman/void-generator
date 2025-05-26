package swiss.sib.swissprot.voidcounter;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Consumer;

import org.eclipse.rdf4j.repository.Repository;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.ServiceDescription;

public record CommonGraphVariables(ServiceDescription sd, GraphDescription gd, Repository repository,
		Consumer<ServiceDescription> saver, ReadWriteLock rwLock, Semaphore limiter, AtomicInteger finishedQueries) implements Variables{

	@Override
	public Lock readLock() {
		return rwLock.readLock();
	}

	@Override
	public Lock writeLock() {
		return rwLock.writeLock();
	}

	public void save() {
		saver.accept(sd);
	}
}
