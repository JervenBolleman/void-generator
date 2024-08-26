package swiss.sib.swissprot.voidcounter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.PredicatePartition;

public class IsSourceClassLinkedToTargetClassTest {

    private IsSourceClassLinkedToTargetClass isSourceClassLinkedToTargetClass;
    private Repository repository;
    private IRI predicate;
    private ClassPartition target;
    private PredicatePartition predicatePartition;
    private ClassPartition source;
    private GraphDescription gd;
    private Lock writeLock;
    private AtomicInteger finishedQueries;
    private Semaphore limiter;
    private AtomicInteger scheduledQueries;

    @BeforeEach
    public void setup() {
        repository = new SailRepository(new MemoryStore());
        repository.init();
        predicate = SimpleValueFactory.getInstance().createIRI("http://example.com/predicate");
        target = new ClassPartition(SimpleValueFactory.getInstance().createIRI("http://example.com/target"));
        predicatePartition = new PredicatePartition();
        source = new ClassPartition(SimpleValueFactory.getInstance().createIRI("http://example.com/source"));
        gd = new GraphDescription();
        gd.setGraphName("http://example.com/graph");
        writeLock = new ReentrantLock();
        finishedQueries = new AtomicInteger(0);
        limiter = new Semaphore(1);
        scheduledQueries = new AtomicInteger(0);

        isSourceClassLinkedToTargetClass = new IsSourceClassLinkedToTargetClass(repository, predicate, target,
                predicatePartition, source, gd, writeLock, limiter, scheduledQueries, finishedQueries);
    }

    @Test
    public void testRun() throws Exception {
        Exception call = isSourceClassLinkedToTargetClass.call();
        assertNull(call);
        assertEquals(0, gd.getPredicates().size());
    }
}
