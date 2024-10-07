package swiss.sib.swissprot.vocabulary;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.base.InternedIRI;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;

/**
 * Namespace Formats. Prefix: {@code <http://www.w3.org/ns/sparql-service-description#>}
 *
 * @author Jerven Bolleman
 * @see <a href="http://www.w3.org/TR/sparql11-service-description/">SPARQL 1.1 Service Description</a>
 */
public class FORMATS {
	public static final String NAMESPACE = "http://www.w3.org/ns/formats/";
	public static final String PREFIX = "formats";
	
	public static final IRI RDF_XML = new InternedIRI(NAMESPACE, "RDF_XML");
	public static final IRI TURTLE = new InternedIRI(NAMESPACE, "Turtle");
	public static final IRI NTRIPLES = new InternedIRI(NAMESPACE, "N-Triples");
	public static final IRI XML = new InternedIRI(NAMESPACE, "SPARQL_Results_XML");
	public static final IRI JSON = new InternedIRI(NAMESPACE, "SPARQL_Results_JSON");
	public static final IRI TSV = new InternedIRI(NAMESPACE, "SPARQL_Results_TSV");
	public static final IRI CSV = new InternedIRI(NAMESPACE, "SPARQL_Results_CSV");
	public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);

}
