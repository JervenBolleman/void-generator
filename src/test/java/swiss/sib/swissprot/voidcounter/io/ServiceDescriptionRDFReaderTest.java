package swiss.sib.swissprot.voidcounter.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.StringReader;
import java.time.LocalDate;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.UnsupportedRDFormatException;
import org.junit.jupiter.api.Test;

import swiss.sib.swissprot.servicedescription.ServiceDescription;
import swiss.sib.swissprot.servicedescription.io.ServiceDescriptionRDFReader;

public class ServiceDescriptionRDFReaderTest {
	private static final ValueFactory VF = SimpleValueFactory.getInstance();
	private static final String EX = "https://example.org/";

	@Test
	public void simple() throws RDFParseException, UnsupportedRDFormatException, IOException {
		String simpleTestTtl = """
				@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
				@prefix void: <http://rdfs.org/ns/void#> .
				@prefix : <http://www.w3.org/ns/sparql-service-description#> .
				@prefix void_ext: <http://ldf.fi/void-ext#> .
				@prefix formats: <http://www.w3.org/ns/formats/> .
				@prefix pav: <http://purl.org/pav/> .
				@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
				@prefix dcterms: <http://purl.org/dc/terms/> .

				<https://example.org/sparql> a :Service;
				  :defaultDataset <https://example.org/sparql#sparql-default-dataset>;
				  :endpoint <https://example.org/sparql>;
				  :resultFormat formats:SPARQL_Results_CSV, formats:SPARQL_Results_JSON, formats:N-Triples,
				    formats:RDF_XML, formats:SPARQL_Results_TSV, formats:Turtle, formats:SPARQL_Results_XML;
				  :supportedLanguage :SPARQL11Query;
				  :feature :UnionDefaultGraph, :BasicFederatedQuery .

				<https://example.org/sparql#sparql-default-dataset> a :Dataset;
				  :defaultGraph <https://example.org/.well-known/void#sparql-default-graph>;
				  pav:version "2022";
				  dcterms:issued "2024-06-06"^^xsd:date .

				<https://example.org/.well-known/void#sparql-default-graph> a :Graph;
				  dcterms:title "test title";
				  dcterms:issued "2024-06-06"^^xsd:date .
				""";
		Model simple = Rio.parse(new StringReader(simpleTestTtl), RDFFormat.TURTLE);
		assertFalse(simple.isEmpty());

		ServiceDescription sd = ServiceDescriptionRDFReader.read(simple);
		assertEquals("test title", sd.getTitle());
		assertEquals(LocalDate.of(2024, 06, 06), sd.getReleaseDate());
		assertEquals("2022", sd.getVersion());

		assertTrue(sd.getGraphs().isEmpty());
	}

	@Test
	public void slightlyMoreComplex() throws RDFParseException, UnsupportedRDFormatException, IOException {
		String slightlyMoreComplexTtl = """
				@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
				@prefix void: <http://rdfs.org/ns/void#> .
				@prefix : <http://www.w3.org/ns/sparql-service-description#> .
				@prefix void_ext: <http://ldf.fi/void-ext#> .
				@prefix formats: <http://www.w3.org/ns/formats/> .
				@prefix pav: <http://purl.org/pav/> .
				@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
				@prefix dcterms: <http://purl.org/dc/terms/> .
				
				<https://example.org/sparql> a :Service;
				  :defaultDataset <https://example.org/sparql#sparql-default-dataset>;
				  :endpoint <https://example.org/sparql>;
				  :resultFormat formats:SPARQL_Results_CSV, formats:SPARQL_Results_JSON, formats:N-Triples,
				    formats:RDF_XML, formats:SPARQL_Results_TSV, formats:Turtle, formats:SPARQL_Results_XML;
				  :supportedLanguage :SPARQL11Query;
				  :feature :UnionDefaultGraph, :BasicFederatedQuery .
				
				<https://example.org/sparql#sparql-default-dataset> a :Dataset;
				  :defaultGraph <https://example.org/.well-known/void#sparql-default-graph>;
				  pav:version "2022";
				  dcterms:issued "2024-06-06"^^xsd:date;
				  :namedGraph <https://example.org/test_graph> .
				
				<https://example.org/.well-known/void#sparql-default-graph> a :Graph;
				  dcterms:title "test title";
				  dcterms:issued "2024-06-06"^^xsd:date .
				
				<https://example.org/test_graph> :name <https://example.org/test_graph>;
				  :graph <https://example.org/.well-known/void#_graph_test_graph> .
				
				<https://example.org/.well-known/void#_graph_test_graph> a :Graph;
				  void:entities "0"^^xsd:long;
				  void:classes "1"^^xsd:long;
				  void:classPartition <https://example.org/.well-known/void#test_graph!954c4977bb7f807aff75c0541673b595!Bag>;
				  void:distinctObjects "1"^^xsd:long;
				  void_ext:distinctBlankNodeObjects "1"^^xsd:long .
				
				<https://example.org/.well-known/void#test_graph!954c4977bb7f807aff75c0541673b595!Bag>
				  a void:Dataset;
				  void:class rdf:Bag .
				""";

		Model slightlyMoreComplex = Rio.parse(new StringReader(slightlyMoreComplexTtl), RDFFormat.TURTLE);
		ServiceDescription sd = ServiceDescriptionRDFReader.read(slightlyMoreComplex);
		assertEquals("test title", sd.getTitle());
		assertEquals(LocalDate.of(2024, 06, 06), sd.getReleaseDate());
		assertEquals("2022", sd.getVersion());

	}
}
