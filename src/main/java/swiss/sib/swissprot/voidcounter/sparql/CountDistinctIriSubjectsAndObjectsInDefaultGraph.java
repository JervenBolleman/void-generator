package swiss.sib.swissprot.voidcounter.sparql;

import java.util.Iterator;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

final class CountDistinctIriSubjectsAndObjectsInDefaultGraph extends
		QueryCallable<SubObjCount> {

	private static final Logger log = LoggerFactory.getLogger(CountDistinctIriSubjectsAndObjectsInDefaultGraph.class);
	private final CommonVariables cv;
	private final String query;

	public CountDistinctIriSubjectsAndObjectsInDefaultGraph(CommonVariables cv, OptimizeFor optimizeFor) {
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;
		query = Helper.loadSparqlQuery("count_distinct_subjects_objects", optimizeFor);
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct iri objects and subjects for default graph");
	}

	@Override
	protected void logFailed(Exception e) {
		if (log.isErrorEnabled())
			log.error("failed counting distinct iri objects and subjects default graph", e);
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct iri objects and subjects for graph default graph");
	}

	@Override
	protected SubObjCount run(RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {

		setQuery(query);
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
			cv.sd().setDistinctIriSubjectCount(t.subjects());
			cv.sd().setDistinctIriObjectCount(t.objects());
		} finally {
			cv.writeLock().unlock();
		}
	}

	@Override
	protected Logger getLog() {
		return log;
	}

}
