package swiss.sib.swissprot.voidcounter.sparql;

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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.servicedescription.ServiceDescription;
import swiss.sib.swissprot.voidcounter.CommonGraphVariables;

public class IsSourceClassLinkedToDistinctClassInOtherGraphsTest {

	private Repository repository;
	private final IRI predicate  = SimpleValueFactory.getInstance().createIRI("http://example.com/predicate");
	private final IRI source = SimpleValueFactory.getInstance().createIRI("http://example.com/source");
	private final IRI target = SimpleValueFactory.getInstance().createIRI("http://example.com/target");

	private ClassPartition sourceClass = new ClassPartition(
			SimpleValueFactory.getInstance().createIRI("http://example.com/sourceClass"));
	private ClassPartition targetClass = new ClassPartition(
			SimpleValueFactory.getInstance().createIRI("http://example.com/targetClass"));;
	private PredicatePartition predicatePartition;
	private GraphDescription sourceGraph;
	private Lock writeLock;
	private AtomicInteger finishedQueries;
	private Semaphore limiter;
	private GraphDescription targetGraph;
	private ServiceDescription sd;
	private CommonGraphVariables cv;

	@BeforeEach
	public void setup() {
		repository = new SailRepository(new MemoryStore());
		repository.init();
		predicatePartition = new PredicatePartition(predicate);
		
		sourceGraph = new GraphDescription();
		sourceGraph.setGraphName("http://example.com/graph");
		targetGraph = new GraphDescription();
		targetGraph.setGraphName("http://example.com/otherGraph");
		writeLock = new ReentrantLock();
		finishedQueries = new AtomicInteger(0);
		limiter = new Semaphore(1);
		sd = new ServiceDescription();
		sd.putGraphDescription(targetGraph);
		sd.putGraphDescription(sourceGraph);
		addTestData();
		cv = new CommonGraphVariables(sd , sourceGraph, repository, s->{}, writeLock, limiter, finishedQueries);
	}

	@ParameterizedTest
	@EnumSource(OptimizeFor.class) 
	public void testRun(OptimizeFor of) throws Exception {
		var isSourceClassLinkedToTargetClass = new IsSourceClassLinkedToDistinctClassInOtherGraphs(cv, 
				predicatePartition, sourceClass, null, of);
		try (RepositoryConnection connection = repository.getConnection()) {
			SimpleValueFactory vf = SimpleValueFactory.getInstance();
			connection.begin();
			connection.add(vf.createStatement(source, RDF.TYPE, sourceClass.getClazz(), sourceGraph.getGraph()));
			connection.add(vf.createStatement(source, predicate, source, sourceGraph.getGraph()), sourceGraph.getGraph());
			connection.add(vf.createStatement(target, RDF.TYPE, targetClass.getClazz(), RDFS.COMMENT));
			connection.commit();
		}
		Exception call = isSourceClassLinkedToTargetClass.call();
		assertNull(call);
		assertEquals(1, predicatePartition.getLinkSets().size());
	}
	
	@ParameterizedTest
	@EnumSource(OptimizeFor.class) 
	public void testRunExclude(OptimizeFor fr) throws Exception {
		var isSourceClassLinkedToTargetClass = new IsSourceClassLinkedToDistinctClassInOtherGraphs(cv,  
				predicatePartition, sourceClass, "strStarts(str(?clazz), 'http://example.com/')", fr);
		Exception call = isSourceClassLinkedToTargetClass.call();
		assertNull(call);
		assertEquals(1, predicatePartition.getLinkSets().size());
	}
	
	@ParameterizedTest
	@EnumSource(OptimizeFor.class) 
	public void testWithOtherKnown(OptimizeFor of) throws Exception {
		targetGraph.getClasses().add(new ClassPartition(targetClass.getClazz()));
		var isSourceClassLinkedToTargetClass = new IsSourceClassLinkedToDistinctClassInOtherGraphs(cv, 
				predicatePartition, sourceClass, "strStarts(str(?clazz), 'http://example.com/')", of);		
		Exception call = isSourceClassLinkedToTargetClass.call();
		assertNull(call);
		assertEquals(1, predicatePartition.getLinkSets().size());
	}

	protected void addTestData() {
		try (RepositoryConnection connection = repository.getConnection()) {
			SimpleValueFactory vf = SimpleValueFactory.getInstance();
			connection.begin();
			connection.add(vf.createStatement(source, RDF.TYPE, sourceClass.getClazz(), sourceGraph.getGraph()));
			connection.add(vf.createStatement(source, predicate, target, sourceGraph.getGraph()));
			connection.add(vf.createStatement(target, RDF.TYPE, targetClass.getClazz(), targetGraph.getGraph()));
			connection.commit();
		}
	}
}
