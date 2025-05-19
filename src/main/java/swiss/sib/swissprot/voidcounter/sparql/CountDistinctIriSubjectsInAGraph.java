package swiss.sib.swissprot.voidcounter.sparql;

import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

final class CountDistinctIriSubjectsInAGraph extends QueryCallable<Long> {

	private static final String SUBJECTS = "subjects";
	private final String rq;
	private static final Logger log = LoggerFactory.getLogger(CountDistinctIriSubjectsInAGraph.class);


	public CountDistinctIriSubjectsInAGraph(CommonVariables cv, OptimizeFor opti) {
		super(cv);
		this.rq = Helper.loadSparqlQuery("count_distinct_iri_subjects_in_all_graphs",
				opti);
		
	}

	protected Long run(RepositoryConnection connection) throws Exception {
		var bindings = new MapBindingSet();
		bindings.addBinding("graph", cv.gd().getGraph());
		setQuery(rq, bindings);
		return Helper.getSingleLongFromSparql(getQuery(), connection, SUBJECTS);
	}

	@Override
	protected void logFailed(Exception e) {
		if (log.isErrorEnabled()) {
			log.error("Failed to run query to count distinct IRI subjects for " + cv.gd().getGraphName(), e);
		}
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct iri subjects for {}", cv.gd().getGraphName());
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct iri subjects for {}", cv.gd().getGraphName());
	}

	@Override
	protected void set(Long count) {
		try {
			cv.writeLock().lock();
			cv.gd().setDistinctIriSubjectCount(count);
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