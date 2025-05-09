package swiss.sib.swissprot.voidcounter.sparql;

import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

final class CountDistinctIriSubjectsAndObjectsInAGraph extends
		QueryCallable<swiss.sib.swissprot.voidcounter.sparql.CountDistinctIriSubjectsAndObjectsInAGraph.SubObj> {

	private static final Logger log = LoggerFactory.getLogger(CountDistinctIriSubjectsAndObjectsInAGraph.class);
	private final CommonVariables cv;
	private final String query;

	public CountDistinctIriSubjectsAndObjectsInAGraph(CommonVariables cv, OptimizeFor optimizeFor) {
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;
		query = Helper.loadSparqlQuery("count_distinct_subjects_objects", optimizeFor);
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

	public static record SubObj(Long subjects, Long objects) {

	}

	@Override
	protected SubObj run(RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		long objects = 0;
		long subjects = 0;

		MapBindingSet bs = new MapBindingSet();
		bs.addBinding("graph", cv.gd().getGraph());
		setQuery(query, bs);
		return new SubObj(subjects, objects);
	}

	@Override
	protected void set(SubObj t) {
		cv.save();
		try {
			cv.writeLock().lock();
			cv.gd().setDistinctIriSubjectCount(t.subjects());
			cv.gd().setDistinctIriObjectCount(t.objects);
		} finally {
			cv.writeLock().unlock();
		}
	}

	@Override
	protected Logger getLog() {
		return log;
	}

}
