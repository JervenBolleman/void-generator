package swiss.sib.swissprot.voidcounter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.PredicatePartition;

public class IsSourceClassLinkedToDistinctClassInOtherGraphTest {

	private Repository repository;
	private IRI predicate;
	private final IRI source = SimpleValueFactory.getInstance().createIRI("http://example.com/source");
	private final IRI target = SimpleValueFactory.getInstance().createIRI("http://example.com/target");

	private ClassPartition sourceClass = new ClassPartition(
			SimpleValueFactory.getInstance().createIRI("http://example.com/sourceClass"));
	private ClassPartition targetClass = new ClassPartition(
			SimpleValueFactory.getInstance().createIRI("http://example.com/targetClass"));;
	private PredicatePartition predicatePartition;
	private GraphDescription gd;
	private Lock writeLock;
	private AtomicInteger finishedQueries;
	private Semaphore limiter;
	private GraphDescription ogd;

	@BeforeEach
	public void setup() {
		repository = new SailRepository(new MemoryStore());
		repository.init();
		predicate = SimpleValueFactory.getInstance().createIRI("http://example.com/predicate");
		predicatePartition = new PredicatePartition(predicate);
		
		gd = new GraphDescription();
		gd.setGraphName("http://example.com/graph");
		ogd = new GraphDescription();
		ogd.setGraphName("http://example.com/otherGraph");
		writeLock = new ReentrantLock();
		finishedQueries = new AtomicInteger(0);
		limiter = new Semaphore(1);
		addTestData();
	}

	@Test
	public void testRun() throws Exception {
		IsSourceClassLinkedToDistinctClassInOtherGraph isSourceClassLinkedToTargetClass = new IsSourceClassLinkedToDistinctClassInOtherGraph(repository, 
				predicatePartition, sourceClass, gd, writeLock, limiter, finishedQueries, ogd, (s) -> null, null);
		try (RepositoryConnection connection = repository.getConnection()) {
			SimpleValueFactory vf = SimpleValueFactory.getInstance();
			connection.begin();
			connection.add(vf.createStatement(source, RDF.TYPE, sourceClass.getClazz(), gd.getGraph()));
			connection.add(vf.createStatement(source, predicate, source, gd.getGraph()), gd.getGraph());
			connection.add(vf.createStatement(target, RDF.TYPE, targetClass.getClazz(), RDFS.COMMENT));
			connection.commit();
		}
		Exception call = isSourceClassLinkedToTargetClass.call();
		assertNull(call);
		assertEquals(1, predicatePartition.getLinkSets().size());
	}
	
	@Test
	public void testRunExclude() throws Exception {
		IsSourceClassLinkedToDistinctClassInOtherGraph isSourceClassLinkedToTargetClass = new IsSourceClassLinkedToDistinctClassInOtherGraph(repository, 
				predicatePartition, sourceClass, gd, writeLock, limiter, finishedQueries, ogd, (s) -> null, "strStarts(str(?clazz), 'http://example.com/')");
		Exception call = isSourceClassLinkedToTargetClass.call();
		assertNull(call);
		assertEquals(1, predicatePartition.getLinkSets().size());
	}
	
	@Test
	public void testWithOtherKnown() throws Exception {
		ogd.getClasses().add(new ClassPartition(targetClass.getClazz()));
		IsSourceClassLinkedToDistinctClassInOtherGraph isSourceClassLinkedToTargetClass = new IsSourceClassLinkedToDistinctClassInOtherGraph(repository, 
				predicatePartition, sourceClass, gd, writeLock, limiter, finishedQueries, ogd, (s) -> null, "strStarts(str(?clazz), 'http://example.com/')");		
		Exception call = isSourceClassLinkedToTargetClass.call();
		assertNull(call);
		assertEquals(1, predicatePartition.getLinkSets().size());
	}

	protected void addTestData() {
		try (RepositoryConnection connection = repository.getConnection()) {
			SimpleValueFactory vf = SimpleValueFactory.getInstance();
			connection.begin();
			connection.add(vf.createStatement(source, RDF.TYPE, sourceClass.getClazz(), gd.getGraph()));
			connection.add(vf.createStatement(source, predicate, target, gd.getGraph()));
			connection.add(vf.createStatement(target, RDF.TYPE, targetClass.getClazz(), ogd.getGraph()));
			connection.commit();
		}
	}
}
