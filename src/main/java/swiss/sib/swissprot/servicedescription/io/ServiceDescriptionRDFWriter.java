package swiss.sib.swissprot.servicedescription.io;

import java.io.OutputStream;
import java.util.Optional;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.DCTERMS;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;

import swiss.sib.swissprot.servicedescription.ServiceDescription;
import swiss.sib.swissprot.servicedescription.ServiceDescriptionStatementGenerator;
import swiss.sib.swissprot.vocabulary.FORMATS;
import swiss.sib.swissprot.vocabulary.PAV;
import swiss.sib.swissprot.vocabulary.VOID_EXT;

public class ServiceDescriptionRDFWriter {

	public static void write(ServiceDescription sdg, IRI iriOfVoid, RDFFormat f, OutputStream os, IRI iriOfEndpoint) {
		RDFWriter rh = Rio.createWriter(f, os);

		rh.startRDF();
		rh.handleNamespace(RDF.PREFIX, RDF.NAMESPACE);
		rh.handleNamespace(VOID.PREFIX, VOID.NAMESPACE);
		rh.handleNamespace("", SD.NAMESPACE);
		rh.handleNamespace(VOID_EXT.PREFIX, VOID_EXT.NAMESPACE);
		rh.handleNamespace(FORMATS.PREFIX, FORMATS.NAMESPACE);
		rh.handleNamespace(PAV.PREFIX, PAV.NAMESPACE);
		rh.handleNamespace(VOID_EXT.PREFIX, VOID.NAMESPACE);
		rh.handleNamespace(XSD.PREFIX, XSD.NAMESPACE);
		rh.handleNamespace(DCTERMS.PREFIX, DCTERMS.NAMESPACE);

		new ServiceDescriptionStatementGenerator(rh).generateStatements(iriOfEndpoint, iriOfVoid, sdg);
		rh.endRDF();
	}

}
