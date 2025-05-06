package swiss.sib.swissprot.servicedescription.sparql;

import java.io.IOException;
import java.util.Iterator;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.UnsupportedRDFormatException;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.vocabulary.SHACL;

public class Helper {

	private Helper() {

	}

	public static long getSingleLongFromSparql(TupleQuery tq, RepositoryConnection connection, String variable)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		try (TupleQueryResult res = tq.evaluate()) {
			if (res.hasNext()) {
				Binding bind = res.next().getBinding(variable);
				assert bind.getValue().isLiteral();
				assert !res.hasNext();
				return ((Literal) bind.getValue()).longValue();
			} else {
				return 0;
			}
		}
	}

	public static long getSingleLongFromSparql(String sq, RepositoryConnection connection, String variable)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		try (TupleQueryResult res = runTupleQuery(sq, connection)) {
			if (res.hasNext()) {
				Binding bind = res.next().getBinding(variable);
				assert bind.getValue().isLiteral();
				assert !res.hasNext();
				return ((Literal) bind.getValue()).longValue();
			} else {
				return 0;
			}
		}
	}

	public static TupleQueryResult runTupleQuery(String sq, RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		TupleQuery q = connection.prepareTupleQuery(QueryLanguage.SPARQL, sq);
		return q.evaluate();
	}

	public static String loadSparqlQuery(String queryFileName) {
		Model model = new LinkedHashModel();
		try (var in = Helper.class.getClassLoader().getResourceAsStream(queryFileName + ".ttl")) {
			model = Rio.parse(in, RDFFormat.TURTLE);
		} catch (RDFParseException | UnsupportedRDFormatException | IOException e) {
			throw new IllegalStateException("Failed to load SPARQL query from " + queryFileName, e);
		}
		Iterator<Statement> iterator = model.filter(null, SHACL.SELECT, null).iterator();
		if (iterator.hasNext()) {
			return iterator.next().getObject().stringValue();
		}
		throw new IllegalStateException("Failed to load SPARQL query from " + queryFileName);
	}

}
