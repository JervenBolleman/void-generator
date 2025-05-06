package swiss.sib.swissprot.voidcounter;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.ObjectPartition;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.servicedescription.ServiceDescription;
import swiss.sib.swissprot.servicedescription.sparql.Helper;

public class FindNamedIndividualObjectSubjectForPredicateInGraph extends QueryCallable<Set<ObjectPartition>> {
	private static final String QUERY = Helper
			.loadSparqlQuery("count_subjects_with_a_type_and_predicate_to_named_individual");
	private static final Logger log = LoggerFactory
			.getLogger(FindNamedIndividualObjectSubjectForPredicateInGraph.class);

	private final PredicatePartition predicatePartition;
	private final GraphDescription gd;
	private final ClassPartition cp;
	private final Lock writeLock;
	private final Consumer<ServiceDescription> saver;
	private final ServiceDescription sd;

	public FindNamedIndividualObjectSubjectForPredicateInGraph(ServiceDescription sd, GraphDescription gd,
			PredicatePartition predicatePartition, ClassPartition cp, Repository repository,
			Consumer<ServiceDescription> saver, Lock writeLock, Semaphore limiter, AtomicInteger finishedQueries) {
		super(repository, limiter, finishedQueries);
		this.sd = sd;
		this.gd = gd;
		this.predicatePartition = predicatePartition;
		this.cp = cp;
		this.saver = saver;
		this.writeLock = writeLock;
	}

	@Override
	protected void logStart() {
		log.debug("Finding if {} with predicate partition {} has subject partitions in {}", cp.getClazz(),
				predicatePartition.getPredicate(), gd.getGraphName());
	}

	@Override
	protected void logEnd() {
		if (!predicatePartition.getSubjectPartitions().isEmpty()) {
			log.debug("{} has predicate partition {} with {} subject partition in {}", cp.getClazz(),
					predicatePartition.getPredicate(), predicatePartition.getSubjectPartitions().size(),
					gd.getGraphName());
		} else {
			log.debug("Found no {} with predicate partition {} has subject partitions in {}", cp.getClazz(),
					predicatePartition.getPredicate(), gd.getGraphName());
		}
	}

	@Override
	protected Set<ObjectPartition> run(RepositoryConnection connection) throws Exception {
		MapBindingSet tq = new MapBindingSet();
		tq.setBinding("graph", gd.getGraph());
		tq.setBinding("sourceType", cp.getClazz());
		tq.setBinding("predicate", predicatePartition.getPredicate());
		setQuery(QUERY, tq);
		Set<ObjectPartition> namedObjects = new HashSet<>();
		try (final TupleQueryResult tr = Helper.runTupleQuery(getQuery(), connection)) {
			while (tr.hasNext()) {
				final BindingSet next = tr.next();
				if (next.hasBinding("target")) {
					Value v = next.getBinding("target").getValue();
					Value count = next.getBinding("count").getValue();
					
					if (v instanceof IRI i && count instanceof  Literal c) {
						ObjectPartition subTarget = new ObjectPartition(i);
						subTarget.setTripleCount(c.longValue());
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
		if (!namedObjects.isEmpty())
			saver.accept(sd);
	}

	@Override
	protected Logger getLog() {
		return log;
	}
}