package swiss.sib.swissprot.servicedescription;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;

import swiss.sib.swissprot.servicedescription.sparql.Helper;

public class FindGraphs {
	
	private FindGraphs() {
		// Prevent instantiation
	}

	private static final String PREFFERED_QUERY = "SELECT DISTINCT ?g WHERE {GRAPH ?g {} }";
	
	private static final String FALLBACK_QUERY = "SELECT DISTINCT ?g WHERE {GRAPH ?g { ?s ?p ?o}}";
	
	private static final Set<String> VIRTUOSO_GRAPHS = Set.of("http://www.openlinksw.com/schemas/virtrdf#",
			"http://www.w3.org/ns/ldp#", "urn:activitystreams-owl:map", "urn:core:services:sparql");

	public static Set<String> findAllNonVirtuosoGraphs(RepositoryConnection connection,
			AtomicInteger scheduledQueries, AtomicInteger finishedQueries)  {
		Set<String> res = new HashSet<>();
		findGraphs(connection, res, PREFFERED_QUERY, scheduledQueries, finishedQueries);
		if (res.isEmpty()) {
			findGraphs(connection, res, FALLBACK_QUERY, scheduledQueries, finishedQueries);
		}
		
		return res;

	}

	private static void findGraphs(RepositoryConnection connection,
			Set<String> res, String query, AtomicInteger scheduledQueries, AtomicInteger finishedQueries) {
		scheduledQueries.incrementAndGet();
		try (final TupleQueryResult foundGraphs = Helper
				.runTupleQuery(query, connection)) {
			while (foundGraphs.hasNext()) {
				final BindingSet next = foundGraphs.next();
				Binding binding = next.getBinding("g");
				if (binding != null) {
					final String graphIRI = binding.getValue().stringValue();
					if (!VIRTUOSO_GRAPHS.contains(graphIRI))
						res.add(graphIRI);
				}
			}
		} catch(RDF4JException e){
			//Ignore this failure!
		} finally {
			finishedQueries.incrementAndGet();
		}
	}

}
