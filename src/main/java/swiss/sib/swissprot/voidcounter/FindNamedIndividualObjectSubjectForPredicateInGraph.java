package swiss.sib.swissprot.voidcounter;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.servicedescription.SubjectPartition;
import swiss.sib.swissprot.virtuoso.VirtuosoFromSQL;

public class FindNamedIndividualObjectSubjectForPredicateInGraph extends QueryCallable<Set<IRI>> {

	public static final Logger log = LoggerFactory.getLogger(FindNamedIndividualObjectSubjectForPredicateInGraph.class);

	private final PredicatePartition predicatePartition;
	private final GraphDescription gd;
	private final ClassPartition cp;
	private final Lock writeLock;

	public FindNamedIndividualObjectSubjectForPredicateInGraph(GraphDescription gd,
			PredicatePartition predicatePartition, ClassPartition cp, Repository repository, Lock writeLock,
			Semaphore limiter) {
		super(repository, limiter);
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
	protected Set<IRI> run(RepositoryConnection connection) throws Exception {
		final String query = "SELECT DISTINCT ?target  WHERE { GRAPH <" + gd.getGraphName() + "> { ?subject a <"
				+ cp.getClazz() + "> ; <" + predicatePartition.getPredicate() + "> ?target . ?target a <" + OWL.NAMEDINDIVIDUAL + ">}";
		try (final TupleQueryResult tr = VirtuosoFromSQL.runTupleQuery(query, connection)) {
			Set<IRI> namedObjects = new HashSet<>();
			while (tr.hasNext()) {
				final BindingSet next = tr.next();
				if (next.hasBinding("target")) {
					Value v = next.getBinding("target").getValue();
					if (v.isIRI()) {
						namedObjects.add((IRI) v);
					}
				}
			}
			return namedObjects;
		}
	}

	@Override
	protected void set(Set<IRI> namedObjects) {
		for (IRI namedObject : namedObjects) {
			try {
				writeLock.lock();
				SubjectPartition subTarget = new SubjectPartition(namedObject);
				predicatePartition.putSubjectPartition(subTarget);
			} finally {
				writeLock.unlock();
			}
		}
	}
}