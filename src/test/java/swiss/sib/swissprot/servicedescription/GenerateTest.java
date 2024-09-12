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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
			URL resource = GenerateTest.class.getResource("/tutorial.trig");
			connection.add(resource, RDFFormat.TRIG);
			connection.commit();
		}
	
	}

	@AfterEach
	public void shutdownSail() {
		sr.shutDown();
	}

	@Test
	public void generate() throws Exception {
		Generate g = new Generate();
		assertTrue(tempDir.isDirectory());
		g.setGraphNames(new HashSet<>());
		g.setSdFile(new File(tempDir, "void.ttl"));
		g.setIriOfVoidAsString("https://example.org/.well-known/void");
		g.setRepository(sr);
		g.update();
		assertEquals(g.finishedQueries.get(), g.scheduledQueries.get());
	}
}
