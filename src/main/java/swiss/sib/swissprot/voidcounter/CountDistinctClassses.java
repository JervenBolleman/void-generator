package swiss.sib.swissprot.voidcounter;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.virtuoso.VirtuosoFromSQL;

public final class CountDistinctClassses extends QueryCallable<List<ClassPartition>> {
	private final GraphDescription gd;
	private static final Logger log = LoggerFactory.getLogger(CountDistinctClassses.class);
	private final Lock writeLock;

	public CountDistinctClassses(GraphDescription gd, Repository repository, Lock writeLock, Semaphore limiter) {
		super(repository, limiter);
		this.gd = gd;
		this.writeLock = writeLock;
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct classes for " + gd.getGraphName());
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("failed counting distinct classses " + gd.getGraphName(), e);
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct classes:" + gd.getDistinctClassesCount() + " for " + gd.getGraphName());

	}

	@Override
	protected List<ClassPartition> run(RepositoryConnection connection)
			throws MalformedQueryException, QueryEvaluationException, RepositoryException {
		List<ClassPartition> classesList = new ArrayList<>();
		try (TupleQueryResult classes = VirtuosoFromSQL.runTupleQuery(
				"SELECT DISTINCT ?clazz FROM <" + gd.getGraphName() + "> WHERE {?thing a ?clazz } ", connection)) {
			while (classes.hasNext()) {
				Binding classesCount = classes.next().getBinding("clazz");
				Value value = classesCount.getValue();
				if (value.isIRI()) {
					final IRI clazz = (IRI) value;
					classesList.add(new ClassPartition(clazz));
				}
			}
		}
		for (ClassPartition cp : classesList) {
			String countTriples = "SELECT (COUNT(?thing) AS ?count) WHERE {GRAPH <" + gd.getGraphName()
					+ "> {?thing a <" + cp.getClazz().stringValue() + "> }}";
			
			try (TupleQueryResult classes = VirtuosoFromSQL
					.runTupleQuery(countTriples, connection)) {
				while (classes.hasNext()) {
					
					Binding classesCount = classes.next().getBinding("count");
					Value value = classesCount.getValue();
					if (value.isLiteral()) {
						Literal lv = (Literal) value;
						cp.setTripleCount(lv.longValue());
					}
				}
			}
		}
		return classesList;
	}

	@Override
	protected void set(List<ClassPartition> count) {
		try {
			writeLock.lock();
			Set<ClassPartition> classes = gd.getClasses();
			classes.clear();
			classes.addAll(count);
		} finally {
			writeLock.unlock();
		}

	}
}