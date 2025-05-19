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

final class CountDistinctBnodeSubjectsInAGraph extends QueryCallable<Long> {
	private static final String SUBJECTS = "subjects";
	private final String rq;
	private static final Logger log = LoggerFactory.getLogger(CountDistinctBnodeSubjectsInAGraph.class);
	
	public CountDistinctBnodeSubjectsInAGraph(CommonVariables cv, OptimizeFor optimizeFor) {
		super(cv);
		
		this.rq = Helper.loadSparqlQuery("count_distinct_bnode_subjects_in_all_graphs", optimizeFor);
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
		setQuery(rq, bindings);
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
