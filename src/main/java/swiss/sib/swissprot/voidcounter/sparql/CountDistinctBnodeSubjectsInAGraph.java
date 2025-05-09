package swiss.sib.swissprot.voidcounter.sparql;

import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

final class CountDistinctBnodeSubjectsInAGraph extends QueryCallable<Long> {
	private static final String SUBJECTS = "subjects";
	private static final String COUNT_DISTINCT_SUBJECT_QUERY_IN_A_GRAPH = Helper.loadSparqlQuery("count_distinct_bnode_subjects_in_all_graphs");
	private static final Logger log = LoggerFactory.getLogger(CountDistinctBnodeSubjectsInAGraph.class);
	
	private final CommonVariables cv;

	public CountDistinctBnodeSubjectsInAGraph(CommonVariables cv) {
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;		
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct bnode subjects for {}", cv.gd().getGraphName());
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct bnode subjects for {}", cv.gd().getGraphName());
	}

	@Override
	protected Long run(RepositoryConnection connection)
			throws QueryEvaluationException, RepositoryException, MalformedQueryException

	{
		MapBindingSet bindings = new MapBindingSet();
		bindings.addBinding("graph", cv.gd().getGraph());
		setQuery(COUNT_DISTINCT_SUBJECT_QUERY_IN_A_GRAPH, bindings);
		return Helper.getSingleLongFromSparql(getQuery(), connection, SUBJECTS);
	}

	@Override
	protected void logFailed(Exception e) {
		log.error("Failed to run query to count distinct BNode subjects", e);
	}

	@Override
	protected void set(Long count) {
		try {
			cv.writeLock().lock();
			cv.gd().setDistinctBnodeSubjectCount(count);
		} finally {
			cv.writeLock().unlock();
		}
		cv.save();
	}
	
	@Override
	protected Logger getLog() {
		return log;
	}
}
