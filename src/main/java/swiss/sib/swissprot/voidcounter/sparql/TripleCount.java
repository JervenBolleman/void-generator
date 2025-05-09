package swiss.sib.swissprot.voidcounter.sparql;

import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

public class TripleCount extends QueryCallable<Long> {
	private static final String COUNT_TRIPLES_IN_GRAPH = Helper.loadSparqlQuery("count_triples_in_named_graphs");
	private static final Logger log = LoggerFactory.getLogger(TripleCount.class);

	private final CommonVariables cv;

	public TripleCount(CommonVariables cv) {
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv =cv;
	}

	@Override
	protected void logStart() {
		log.debug("Finding size of {}", cv.gd().getGraphName());
	}

	@Override
	protected void logEnd() {
		log.debug("Found size of {}", cv.gd().getGraphName());
	}

	protected Long run(RepositoryConnection connection) throws RepositoryException {	
		MapBindingSet bs = new MapBindingSet();
		bs.setBinding("graph", cv.gd().getGraph()); 
		setQuery(COUNT_TRIPLES_IN_GRAPH, bs);
		return Helper.getSingleLongFromSparql(getQuery(), connection, "count");			
	}

	protected void set(Long size) {
		try {
			cv.writeLock().lock();
			cv.gd().setTripleCount(size);
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
