package swiss.sib.swissprot.vocabulary;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.base.InternedIRI;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;


public class PAV {
	public static final String NAMESPACE= "http://purl.org/pav/";
	public static final String PREFIX = "pav";

	public static final IRI VERSION = new InternedIRI(NAMESPACE, "version");
	public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);

}
