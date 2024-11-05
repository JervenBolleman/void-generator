package swiss.sib.swissprot.voidcounter;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.ObjectPartition;
import swiss.sib.swissprot.servicedescription.PredicatePartition;

public class FindNamedIndividualObjectSubjectForPredicateInGraph extends QueryCallable<Set<ObjectPartition>> {
	private static final String QUERY = """
			PREFIX owl: <http://www.w3.org/2002/07/owl#>
			SELECT 
				?target (COUNT(?subject) as ?count)
			WHERE {
				GRAPH ?graph {
					?subject a ?sourceType ; 
						?predicate ?target . 
					?target a owl:NamedIndividual .
				}
			} GROUP BY ?target
			""";
	public static final Logger log = LoggerFactory.getLogger(FindNamedIndividualObjectSubjectForPredicateInGraph.class);

	private final PredicatePartition predicatePartition;
	private final GraphDescription gd;
	private final ClassPartition cp;
	private final Lock writeLock;

	public FindNamedIndividualObjectSubjectForPredicateInGraph(GraphDescription gd,
			PredicatePartition predicatePartition, ClassPartition cp, Repository repository, Lock writeLock,
			Semaphore limiter, AtomicInteger finishedQueries) {
		super(repository, limiter, finishedQueries);
		this.gd = gd;
		this.predicatePartition = predicatePartition;
		this.cp = cp;
		this.writeLock = writeLock;
	}

	@Override
	protected void logStart() {
		log.debug("Finding if " + cp.getClazz() + " with predicate partition " + predicatePartition.getPredicate()
				+ " has subject partitions in " + gd.getGraphName());
	}

	@Override
	protected void logEnd() {
		if (!predicatePartition.getSubjectPartitions().isEmpty()) {
			log.debug(cp.getClazz() + " has predicate partition " + predicatePartition.getPredicate() + " with "
					+ predicatePartition.getSubjectPartitions().size() + "subject partition in " + gd.getGraphName());
		} else {
			log.debug("Found no " + cp.getClazz() + " with predicate partition " + predicatePartition.getPredicate()
					+ " has subject partitions in " + gd.getGraphName());
		}
	}

	@Override
	protected Set<ObjectPartition> run(RepositoryConnection connection) throws Exception {
		TupleQuery tq = connection.prepareTupleQuery(QUERY);
		tq.setBinding("graph", gd.getGraph());
		tq.setBinding("sourceType", cp.getClazz());
		tq.setBinding("predicate", predicatePartition.getPredicate());
		setQuery(QUERY, tq.getBindings());
		Set<ObjectPartition> namedObjects = new HashSet<>();
		try (final TupleQueryResult tr = tq.evaluate()) {
			while (tr.hasNext()) {
				final BindingSet next = tr.next();
				if (next.hasBinding("target")) {
					Value v = next.getBinding("target").getValue();
					long c = ((Literal) next.getBinding("count").getValue()).longValue();
					
					if (v instanceof IRI i) {
						ObjectPartition subTarget = new ObjectPartition(i);
						subTarget.setTripleCount(c);
						namedObjects.add(subTarget);
					}
				}
			}
		}
		return namedObjects;
	}

	@Override
	protected void set(Set<ObjectPartition> namedObjects) {
		try {
			writeLock.lock();
			for (ObjectPartition namedObject : namedObjects) {
				predicatePartition.putSubjectPartition(namedObject);
			}
		} finally {
			writeLock.unlock();
		}
	}
	
	@Override
	protected Logger getLog() {
		return log;
	}
}