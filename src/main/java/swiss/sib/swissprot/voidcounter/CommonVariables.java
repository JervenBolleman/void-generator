package swiss.sib.swissprot.voidcounter;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Consumer;

import org.eclipse.rdf4j.repository.Repository;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.ServiceDescription;

public record CommonVariables(ServiceDescription sd, Repository repository,
		Consumer<ServiceDescription> saver, ReadWriteLock readWriteLock, Semaphore limiter, AtomicInteger finishedQueries) implements Variables{

	public void save() {
		saver.accept(sd);
	}

	public CommonGraphVariables with(GraphDescription gd) {
		return new CommonGraphVariables(sd(), gd, repository(), saver(), readWriteLock(), limiter(), finishedQueries());
	}
	
	
	public Lock writeLock() {
		return readWriteLock.writeLock();
	}
	
	public Lock readLock() {
		return readWriteLock.readLock();
	}
}
