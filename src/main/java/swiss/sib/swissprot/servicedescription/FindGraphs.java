package swiss.sib.swissprot.servicedescription;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;

import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

public class FindGraphs extends QueryCallable<Set<String>> {
	private final OptimizeFor optimizeFor;

	private final AtomicInteger scheduledQueries;

	private final CommonVariables cv;

	public FindGraphs(CommonVariables cv, OptimizeFor optimizeFor, AtomicInteger scheduledQueries) {
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;
		this.optimizeFor = optimizeFor;
		this.scheduledQueries = scheduledQueries;
	}

	private static final Logger log = org.slf4j.LoggerFactory.getLogger(FindGraphs.class);

	private static final Set<String> VIRTUOSO_GRAPHS = Set.of("http://www.openlinksw.com/schemas/virtrdf#",
			"http://www.w3.org/ns/ldp#", "urn:activitystreams-owl:map", "urn:core:services:sparql");

	public Set<String> findAllNonVirtuosoGraphs(RepositoryConnection connection) {
		Set<String> res = new HashSet<>();
		String prefQuery = Helper.loadSparqlQuery("find_graphs_preferred", optimizeFor);
		findGraphs(connection, res, prefQuery, scheduledQueries, finishedQueries);
		if (res.isEmpty()) {
			String fallbackQuery = Helper.loadSparqlQuery("find_graphs_fallback", optimizeFor);
			findGraphs(connection, res, fallbackQuery, scheduledQueries, finishedQueries);
		}

		return res;

	}

	private void findGraphs(RepositoryConnection connection, Set<String> res, String query,
			AtomicInteger scheduledQueries, AtomicInteger finishedQueries) {
		scheduledQueries.incrementAndGet();
		setQuery(query);
		try (final TupleQueryResult foundGraphs = Helper.runTupleQuery(query, connection)) {
			while (foundGraphs.hasNext()) {
				final BindingSet next = foundGraphs.next();
				Binding binding = next.getBinding("g");
				if (binding != null) {
					final String graphIRI = binding.getValue().stringValue();
					if (!VIRTUOSO_GRAPHS.contains(graphIRI))
						res.add(graphIRI);
				}
			}
		} catch (RDF4JException e) {
			// Ignore this failure!
		} finally {
			finishedQueries.incrementAndGet();
		}
	}

	@Override
	protected void logStart() {
		log.debug("Finding all graphs");
	}

	@Override
	protected void logEnd() {
		log.debug("Finding all graphs");

	}

	@Override
	protected Set<String> run(RepositoryConnection connection) throws Exception {

		return findAllNonVirtuosoGraphs(connection);
	}

	@Override
	protected void set(Set<String> t) {
		try {
			cv.writeLock().lock();
			for (String graph : t) {
				GraphDescription gd = new GraphDescription();
				gd.setGraphName(graph);
				cv.sd().putGraphDescription(gd);
			}
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
