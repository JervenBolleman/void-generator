package swiss.sib.swissprot.voidcounter.sparql;

import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

public class TripleCount extends QueryCallable<Long> {
	private final String count;
	private static final Logger log = LoggerFactory.getLogger(TripleCount.class);

	private final CommonVariables cv;

	public TripleCount(CommonVariables cv, OptimizeFor optimizeFor) {
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.count = Helper.loadSparqlQuery("count_triples_in_named_graphs", optimizeFor);
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
		setQuery(count, bs);
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
