package swiss.sib.swissprot.voidcounter.sparql;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Locale;

import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.base.CoreDatatype.RDF;
import org.eclipse.rdf4j.model.vocabulary.SHACL;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import swiss.sib.swissprot.servicedescription.OptimizeFor;

public class SyntaxCheckQueriesTest {

	private final class ReadOnlyQuery implements RDFHandler {
		private Literal query;

		@Override
		public void startRDF() throws RDFHandlerException {
			// TODO Auto-generated method stub

		}

		@Override
		public void handleStatement(Statement st) throws RDFHandlerException {
			if (st.getPredicate().equals(SHACL.SELECT)) {
				query = (Literal) st.getObject();
			}

		}

		@Override
		public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
			// TODO Auto-generated method stub

		}

		@Override
		public void handleComment(String comment) throws RDFHandlerException {
			// TODO Auto-generated method stub

		}

		@Override
		public void endRDF() throws RDFHandlerException {
			// TODO Auto-generated method stub

		}
	}

	@ParameterizedTest
	@EnumSource(value = OptimizeFor.class)
	void check(OptimizeFor of) throws FileNotFoundException, IOException {
		File file = new File("src/main/resources/" + of.toString().toLowerCase(Locale.ROOT));
		if (file.exists() && file.isDirectory()) {
			for (File f : file.listFiles((d, n) -> n.endsWith(".ttl"))) {
				RDFParser parser = Rio.createParser(RDFFormat.TURTLE);

				try (var fis = new FileInputStream(f)) {
					ReadOnlyQuery handler = new ReadOnlyQuery();
					parser.setRDFHandler(handler);
					parser.parse(fis);
					rdf4jCheck(handler.query, f.getName());
				} catch (RDF4JException e) {
					fail(f.getName()+":"+e.getMessage());
				}
			}
		}
	}

	private void rdf4jCheck(Literal query, String filename) {
		SPARQLParser sp = new SPARQLParser();
		try {
			sp.parseQuery(query.stringValue(), RDF.NAMESPACE);
		} catch (RDF4JException e) {
			fail(filename + ':' + e.getMessage());
		}

	}
}
