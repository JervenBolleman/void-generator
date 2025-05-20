package swiss.sib.swissprot.voidcounter.sparql;

import java.util.Iterator;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.voidcounter.CommonGraphVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

final class CountDistinctIriSubjectsAndObjectsInAGraph extends QueryCallable<SubObjCount, CommonGraphVariables> {

	private static final Logger log = LoggerFactory.getLogger(CountDistinctIriSubjectsAndObjectsInAGraph.class);
	private final String query;

	public CountDistinctIriSubjectsAndObjectsInAGraph(CommonGraphVariables cv, OptimizeFor optimizeFor) {
		super(cv);
		query = Helper.loadSparqlQuery("count_distinct_subjects_objects_in_a_graph", optimizeFor);
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct iri objects and subjects for {}", cv.gd().getGraphName());
	}

	@Override
	protected void logFailed(Exception e) {
		if (log.isErrorEnabled())
			log.error("failed counting distinct iri objects and subjects " + cv.gd().getGraphName(), e);
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct iri objects and subjects for graph {}", cv.gd().getGraphName());
	}

	@Override
	protected SubObjCount run(RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {

		MapBindingSet bs = new MapBindingSet();
		bs.addBinding("graph", cv.gd().getGraph());
		setQuery(query, bs);
		try (TupleQueryResult r = Helper.runTupleQuery(getQuery(), connection)) {
			Iterator<BindingSet> iterator = r.iterator();
			if (iterator.hasNext()){
				BindingSet next = iterator.next();
				long subjects = ((Literal) next.getBinding("subjects").getValue()).longValue();
				long objects = ((Literal) next.getBinding("objects").getValue()).longValue();
				return new SubObjCount(subjects, objects);
			} else {
				return new SubObjCount(0L, 0L);
			}
		}
	}

	@Override
	protected void set(SubObjCount t) {
		cv.save();
		try {
			cv.writeLock().lock();
			cv.gd().setDistinctIriSubjectCount(t.subjects());
			cv.gd().setDistinctIriObjectCount(t.objects());
		} finally {
			cv.writeLock().unlock();
		}
	}

	@Override
	protected Logger getLog() {
		return log;
	}

}
