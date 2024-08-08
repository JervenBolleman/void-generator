package swiss.sib.swissprot.voidcounter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.ServiceDescription;
import swiss.sib.swissprot.servicedescription.ShapeForSubclassOfPartition;
import swiss.sib.swissprot.servicedescription.ShapePartition;
import swiss.sib.swissprot.servicedescription.sparql.Helper;

public final class CountDistinctShapeDefiningSuperClasses extends QueryCallable<List<ShapeForSubclassOfPartition>> {
	private final GraphDescription gd;
	private static final Logger log = LoggerFactory.getLogger(CountDistinctShapeDefiningSuperClasses.class);
	private final Lock writeLock;
	private final AtomicInteger finishedQueries;
	private final AtomicInteger scheduledQueries;
	private final Consumer<ServiceDescription> saver;
	private final ServiceDescription sd;

	public CountDistinctShapeDefiningSuperClasses(GraphDescription gd, Repository repository, Lock writeLock, Semaphore limiter,
			AtomicInteger scheduledQueries, AtomicInteger finishedQueries, Consumer<ServiceDescription> saver,
			ServiceDescription sd) {
		super(repository, limiter);
		this.gd = gd;
		this.writeLock = writeLock;
		this.scheduledQueries = scheduledQueries;
		this.finishedQueries = finishedQueries;
		this.saver = saver;
		this.sd = sd;
		scheduledQueries.incrementAndGet();
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
	protected List<ShapeForSubclassOfPartition> run(RepositoryConnection connection)
			throws MalformedQueryException, QueryEvaluationException, RepositoryException {
		List<ShapeForSubclassOfPartition> classesList = new ArrayList<>();
		try (TupleQueryResult classes = Helper.runTupleQuery(
				"SELECT DISTINCT ?superClass WHERE {\n"
				+ "  ?superClass a ?type .\n"
				+ "  GRAPH <"+gd.getGraphName()+">{\n"
				+ "  	?subClass <"+RDFS.SUBCLASSOF.stringValue()+"> ?superClass .\n"
				+ "  	MINUS{\n"
				+ "   		?subClass a ?otherType . \n"
				+ "  	}\n"
				+ "  }\n"
				+ "}", connection)) {
			while (classes.hasNext()) {
				Binding classesCount = classes.next().getBinding("superClazz");
				Value value = classesCount.getValue();
				if (value.isIRI()) {
					final IRI clazz = (IRI) value;
					classesList.add(new ShapeForSubclassOfPartition(clazz));
				}
			}
		} finally {
			finishedQueries.incrementAndGet();
		}
		scheduledQueries.addAndGet(classesList.size());
		for (ShapeForSubclassOfPartition cp : classesList) {
			String countTriples = "SELECT (COUNT(?thing) AS ?count) WHERE {GRAPH <" + gd.getGraphName()
					+ "> { "+cp.getShapeMembersSelectionQuery()+" }}";

			try (TupleQueryResult classes = Helper.runTupleQuery(countTriples, connection)) {
				while (classes.hasNext()) {

					Binding classesCount = classes.next().getBinding("count");
					Value value = classesCount.getValue();
					if (value.isLiteral()) {
						Literal lv = (Literal) value;
						cp.setTripleCount(lv.longValue());
					}
				}
			} finally {
				finishedQueries.incrementAndGet();
			}
		}
		saver.accept(sd);
		return classesList;
	}

	@Override
	protected void set(List<ShapeForSubclassOfPartition> count) {
		try {
			writeLock.lock();
			List<ShapePartition> classes = gd.getShapePartions();
			classes.addAll(count);
		} finally {
			writeLock.unlock();
		}

	}
}