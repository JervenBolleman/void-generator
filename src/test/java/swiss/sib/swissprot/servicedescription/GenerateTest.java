package swiss.sib.swissprot.servicedescription;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashSet;

import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class GenerateTest {


	@TempDir
	private File tempDir;
	private SailRepository sr;

//	@BeforeAll
//	public static void setLoggingToDebug() {
//		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
//	}
	
	
	@BeforeEach
	public void loadTestData() throws RDFParseException, RepositoryException, IOException {
		MemoryStore ms = new MemoryStore();
		sr = new SailRepository(ms);
		sr.init();
		try (SailRepositoryConnection connection = sr.getConnection()) {
			connection.begin();
			URL resource = GenerateTest.class.getResource("tutorial.trig");
			if (resource != null) {
				connection.add(resource, RDFFormat.TRIG);
			}
			connection.commit();
		}
	
	}

	@AfterEach
	public void shutdownSail() {
		sr.shutDown();
	}

	@ParameterizedTest
	@MethodSource("optimizeFor")
	public void generate(String of) throws Exception {
		Generate g = new Generate();
		assertTrue(tempDir.isDirectory());
		g.setGraphNames(new HashSet<>());
		File sdFile = new File(tempDir, "void.ttl");
		g.setSdFile(sdFile);
		g.setIriOfVoidAsString("https://example.org/.well-known/void");
		g.setRepository(sr);
		g.setRepositoryLocator("https://example.org/sparql");
		g.setOptimizeFor(of);
		g.update();
		assertEquals(g.finishedQueries.get(), g.scheduledQueries.get());
		assertTrue(sdFile.exists());
		assertTrue(sdFile.length() > 0);
	}
	
	static String[] optimizeFor() {
		return new String[] { OptimizeFor.QLEVER.name(), OptimizeFor.SPARQL.name(), OptimizeFor.SPARQLUNION.name(), "lala" };
	}
}
