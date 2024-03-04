package swiss.sib.swissprot.servicedescription.sparql;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;

public class Helper {

	private Helper() {
		
	}
	
	public static Value getFirstNumberResultFromTupleQuery(String sq, RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		return Helper.getFirstNumberResultFromTupleQuery(sq, connection, "types");
	}

	public static Value getFirstNumberResultFromTupleQuery(String sq, RepositoryConnection connection, String variable)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		try (TupleQueryResult classes = runTupleQuery(sq, connection)) {
			if (classes.hasNext()) {
				Binding types = classes.next().getBinding(variable);
				return types.getValue();
			} else {
				return SimpleValueFactory.getInstance().createLiteral(0);
			}
		}
	}

	public static TupleQueryResult runTupleQuery(String sq, RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		TupleQuery q = connection.prepareTupleQuery(QueryLanguage.SPARQL, sq);
		return q.evaluate();
	}

}
