package swiss.sib.swissprot.voidcounter.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Collection;
import java.util.Iterator;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.Test;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.ServiceDescription;
import swiss.sib.swissprot.servicedescription.io.ServiceDescriptionRDFWriter;

public class ServiceDescriptionRDFWriterTest {
	private static final ValueFactory VF = SimpleValueFactory.getInstance();
	private static final String EX = "https://example.org/";

	@Test
	public void simple() {
		ServiceDescription sd = new ServiceDescription();
		sd.setTitle("test title");
		sd.setVersion("2022");
		sd.setReleaseDate(LocalDate.of(2024, 6, 6));
		String sdVoid = write(sd);
		assertFalse(sdVoid.isBlank());
		testBasic(sdVoid);
	}

	protected void testBasic(String sdVoid) {
		assertTrue(sdVoid.contains("test title"));
		assertTrue(sdVoid.contains("2022"));
		assertTrue(sdVoid.contains("2024-06-06\"^^xsd:date"));
	}

	protected String write(ServiceDescription sd) {
		IRI voidIri = VF.createIRI(EX, ".well-known/void");
		IRI serviceIri = VF.createIRI(EX, "sparql");
		ByteArrayOutputStream boas = new ByteArrayOutputStream();
		ServiceDescriptionRDFWriter.write(sd, voidIri, RDFFormat.TURTLE, boas, serviceIri);
		String sdVoid = boas.toString(StandardCharsets.UTF_8);
		return sdVoid;
	}

	@Test
	public void slightlyMoreComplex() {
		ServiceDescription sd = new ServiceDescription();
		sd.setTitle("test title");
		sd.setVersion("2022");
		sd.setReleaseDate(LocalDate.of(2024, 6, 6));
		GraphDescription gd = new GraphDescription();
		gd.setCreator(VF.createIRI(EX, "creator"));
		gd.setGraph(VF.createIRI(EX, "graph"));
		gd.setGraphName(VF.createIRI(EX, "test_graph").stringValue());
		gd.setDistinctBnodeObjectCount(1);
		sd.putGraphDescription(gd);

		ClassPartition e = new ClassPartition(RDF.BAG);
		gd.getClasses().add(e);
		String sdVoid = write(sd);
		assertFalse(sdVoid.isBlank());
		testBasic(sdVoid);
		Collection<GraphDescription> graphs = sd.getGraphs();
		assertEquals(1, graphs.size());
		GraphDescription rgd = graphs.iterator().next();
		assertEquals(gd, rgd);
		assertEquals(gd.getDistinctBnodeObjectCount(), rgd.getDistinctBnodeObjectCount());
		assertEquals(gd.getClasses().size(), rgd.getClasses().size());
		Iterator<ClassPartition> gdcpi = gd.getClasses().iterator();
		Iterator<ClassPartition> rgdcpi = rgd.getClasses().iterator();
		while(gdcpi.hasNext() && rgdcpi.hasNext()) {
			ClassPartition gdcp = gdcpi.next();
			ClassPartition rgdcp = rgdcpi.next();
			assertEquals(gdcp, rgdcp);
		}

	}
}
