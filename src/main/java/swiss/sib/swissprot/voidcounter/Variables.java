package swiss.sib.swissprot.voidcounter;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

import org.eclipse.rdf4j.repository.Repository;

import swiss.sib.swissprot.servicedescription.ServiceDescription;

public sealed interface Variables permits CommonGraphVariables, CommonVariables {
	ServiceDescription sd();

	Repository repository();

	Consumer<ServiceDescription> saver();

	Lock readLock();

	Lock writeLock();

	Semaphore limiter();

	AtomicInteger finishedQueries();
}
