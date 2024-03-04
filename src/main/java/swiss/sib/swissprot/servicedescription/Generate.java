package swiss.sib.swissprot.servicedescription;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.rdfxml.RDFXMLParser;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import swiss.sib.swissprot.vocabulary.FORMATS;
import swiss.sib.swissprot.vocabulary.PAV;
import swiss.sib.swissprot.vocabulary.VOID_EXT;
import swiss.sib.swissprot.voidcounter.CountDistinctBnodeObjectsForAllGraphs;
import swiss.sib.swissprot.voidcounter.CountDistinctBnodeSubjects;
import swiss.sib.swissprot.voidcounter.CountDistinctClassses;
import swiss.sib.swissprot.voidcounter.CountDistinctIriObjectsForAllGraphsAtOnce;
import swiss.sib.swissprot.voidcounter.CountDistinctIriObjectsInAGraphVirtuoso;
import swiss.sib.swissprot.voidcounter.CountDistinctIriSubjectsForAllGraphs;
import swiss.sib.swissprot.voidcounter.CountDistinctIriSubjectsInAGraphVirtuoso;
import swiss.sib.swissprot.voidcounter.CountDistinctLiteralObjects;
import swiss.sib.swissprot.voidcounter.CountDistinctLiteralObjectsForAllGraphs;
import swiss.sib.swissprot.voidcounter.FindPredicateLinkSets;
import swiss.sib.swissprot.voidcounter.FindPredicates;
import swiss.sib.swissprot.voidcounter.TripleCount;
import virtuoso.rdf4j.driver.VirtuosoRepository;
import virtuoso.rdf4j.driver.VirtuosoRepositoryConnection;

@Command(name = "void-generate", mixinStandardHelpOptions = true, version = "0.1", description = "Generate a void file")
public class Generate implements Callable<Integer> {

	protected static final Logger log = LoggerFactory.getLogger(Generate.class);

	private static final Set<String> VIRTUOSO_GRAPHS = Set.of("http://www.openlinksw.com/schemas/virtrdf#",
			"http://www.w3.org/ns/ldp#", "urn:activitystreams-owl:map", "urn:core:services:sparql");

	private Set<String> graphNames;
	private Repository repository;

	private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
	private final ServiceDescription sd;

	@Option(names = "--count-distinct-classes", defaultValue = "true")
	private boolean countDistinctClasses = true;
	@Option(names = "--count-distinct-subjects", defaultValue = "true")
	private boolean countDistinctSubjects = true;
	@Option(names = "--count-distinct-objects", defaultValue = "true")
	private boolean countDistinctObjects = true;
	@Option(names = "--find-predicates", defaultValue = "true")
	private boolean findPredicates = true;
	@Option(names = "--count-detailed-void-statistics", defaultValue = "true")
	private boolean detailedCount = true;

	@Option(names = "--ontology-graph-name", description = "ontology graph name (can be added multiple times")
	List<String> ontologyGraphNames;

	@Option(names = { "-s", "--void-file" }, required = true)
	private File sdFile;

	private File distinctSubjectIrisFile;
	private File distinctObjectIrisFile;

	@Option(names = { "-r",
			"--repository" }, description = "A SPARQL http/https endpoint location or a virtuoso jdbc connection string")
	private String repositoryLocator;

	@Option(names = { "-f",
			"--force" }, description = "Force regeneration of the void file discarding previously provided data")
	private boolean forcedRefresh;

	@Option(names = { "-i",
			"--iri-of-void" }, description = "The base IRI where this void file should reside", required = true)
	private String iriOfVoidAsString;
	private IRI iriOfVoid;
	private Set<IRI> knownPredicates;

	@Option(names = { "-g", "--graphs" }, description = "The IRIs of the graphs to query.")
	private String commaSeperatedGraphs;

	@Option(names = { "-p", "--predicates" }, description = "Commas separated IRIs of the predicates in to be counted.")
	private String commaSeperatedKnownPredicates;

	@Option(names = { "--user" }, description = "Virtuoso user", defaultValue = "dba")
	private String user;

	@Option(names = { "--password" }, description = "Virtuoso password", defaultValue = "dba")
	private String password;

	@Option(names = { " --add-void-graph-to-store",
			"-a" }, description = "immediatly attempt to store the void graph into the store and add it to the total count", defaultValue = "false")
	private boolean add = false;

	public static void main(String[] args) {
		int exitCode = new CommandLine(new Generate()).execute(args);
		System.exit(exitCode);
	}

	private final AtomicInteger scheduledQueries = new AtomicInteger();
	private final AtomicInteger finishedQueries = new AtomicInteger();

	private static final ValueFactory VF = SimpleValueFactory.getInstance();

	Pattern comma = Pattern.compile(",", Pattern.LITERAL);

	@Override
	public Integer call() throws Exception {
		this.iriOfVoid = SimpleValueFactory.getInstance().createIRI(iriOfVoidAsString);
		if (commaSeperatedGraphs != null)
			this.graphNames = comma.splitAsStream(commaSeperatedGraphs).collect(Collectors.toSet());
		else
			this.graphNames = new HashSet<>();

		log.debug("Void listener for " + graphNames.stream().collect(Collectors.joining(", ")));
		if (commaSeperatedKnownPredicates != null) {
			this.knownPredicates = comma.splitAsStream(commaSeperatedKnownPredicates).map(s -> VF.createIRI(s))
					.collect(Collectors.toSet());
		} else
			this.knownPredicates = new HashSet<>();
		if (repositoryLocator.startsWith("http")) {
			SPARQLRepository sr = new SPARQLRepository(repositoryLocator);
			sr.enableQuadMode(true);
			sr.setAdditionalHttpHeaders(Map.of("User-Agent", "void-generator"));
			repository = sr;
		} else {
			repository = new VirtuosoRepository(repositoryLocator, user, password);
		}
		this.distinctSubjectIrisFile = new File(sdFile.getParentFile(), "subject-bitsets-per-graph");
		this.distinctObjectIrisFile = new File(sdFile.getParentFile(), "object-bitsets-per-graph");
		update();
		return 0;
	}

	public Generate() {
		super();
		this.sd = new ServiceDescription();
	}

	public void update() {
		// At least 1 but no more than one third of the cpus

		int oneThirdOfCpus = Math.max(1, Runtime.getRuntime().availableProcessors() / 3);
		Semaphore limit = new Semaphore(oneThirdOfCpus);
		ExecutorService executors = Executors.newFixedThreadPool(oneThirdOfCpus);

		ConcurrentHashMap<String, Roaring64Bitmap> distinctSubjectIris = readGraphsWithSerializedBitMaps(
				this.distinctSubjectIrisFile);
		ConcurrentHashMap<String, Roaring64Bitmap> distinctObjectIris = readGraphsWithSerializedBitMaps(
				this.distinctObjectIrisFile);
		Consumer<ServiceDescription> saver = (sdg) -> writeServiceDescriptionAndGraphs(distinctSubjectIris,
				distinctObjectIris, sdg, iriOfVoid);

		List<Future<Exception>> futures = scheduleCounters(executors, sd, saver, distinctSubjectIris,
				distinctObjectIris, limit);

		waitForCountToFinish(futures);
		if (add) {
			log.debug("Starting the count of the void data itself using " + oneThirdOfCpus);
			countTheVoidDataItself(executors, futures, iriOfVoid, saver, distinctSubjectIris, distinctObjectIris,
					repository instanceof VirtuosoRepository, limit);
			waitForCountToFinish(futures);
			saveResults(iriOfVoid, saver);

			log.debug("Starting the count of the void data itself a second time using " + oneThirdOfCpus);

			countTheVoidDataItself(executors, futures, iriOfVoid, saver, distinctSubjectIris, distinctObjectIris,
					repository instanceof VirtuosoRepository, limit);
			waitForCountToFinish(futures);
		}
		saveResults(iriOfVoid, saver);
		executors.shutdown();
	}

	private ConcurrentHashMap<String, Roaring64Bitmap> readGraphsWithSerializedBitMaps(File file) {
		ConcurrentHashMap<String, Roaring64Bitmap> map = new ConcurrentHashMap<>();
		if (file.exists() && !forcedRefresh) {
			try (FileInputStream fis = new FileInputStream(file); ObjectInputStream bis = new ObjectInputStream(fis)) {
				int numberOfGraphs = bis.readInt();
				for (int i = 0; i < numberOfGraphs; i++) {
					String graph = bis.readUTF();
					Roaring64Bitmap rb = new Roaring64Bitmap();
					rb.readExternal(bis);
					map.put(graph, rb);
				}
			} catch (IOException e) {
				log.error("", e);
			}
		}
		return map;
	}

	private void writeServiceDescriptionAndGraphs(ConcurrentHashMap<String, Roaring64Bitmap> distinctSubjectIris,
			ConcurrentHashMap<String, Roaring64Bitmap> distinctObjectIris, ServiceDescription sdg, IRI iriOfVoid) {
		final Lock readLock = rwLock.readLock();
		try {
			readLock.lock();
			Optional<RDFFormat> f = Rio.getWriterFormatForFileName(sdFile.getName());
			try (OutputStream os = new FileOutputStream(sdFile)) {
				RDFWriter rh = Rio.createWriter(f.orElseGet(() -> RDFFormat.TURTLE), os);

				rh.startRDF();
				rh.handleNamespace(RDF.PREFIX, RDF.NAMESPACE);
				rh.handleNamespace(VOID.PREFIX, VOID.NAMESPACE);
				rh.handleNamespace(SD.PREFIX, SD.NAMESPACE);
				rh.handleNamespace(VOID_EXT.PREFIX, VOID_EXT.NAMESPACE);
				rh.handleNamespace(FORMATS.PREFIX, FORMATS.NAMESPACE);
				rh.handleNamespace(PAV.PREFIX, PAV.NAMESPACE);
				rh.handleNamespace(VOID_EXT.PREFIX, VOID.NAMESPACE);
				rh.handleNamespace(XSD.PREFIX, XSD.NAMESPACE);

				new ServiceDescriptionStatementGenerator(rh).generateStatements(iriOfVoid, sdg);
				rh.endRDF();
			} catch (Exception e) {
				log.error("can not store ServiceDescription", e);
			}
			writeGraphsWithSerializedBitMaps(Generate.this.distinctSubjectIrisFile, distinctSubjectIris);
			writeGraphsWithSerializedBitMaps(Generate.this.distinctObjectIrisFile, distinctObjectIris);
		} finally {
			readLock.unlock();
		}
	}

	private void writeGraphsWithSerializedBitMaps(File targetFile, ConcurrentHashMap<String, Roaring64Bitmap> map) {
		if (!targetFile.exists()) {
			try {
				targetFile.createNewFile();
			} catch (IOException e) {
				throw new RuntimeException("Can't create file we need later:", e);
			}
		}
		try (FileOutputStream fos = new FileOutputStream(targetFile);
				BufferedOutputStream bos = new BufferedOutputStream(fos);
				ObjectOutputStream dos = new ObjectOutputStream(bos)) {
			dos.writeInt(map.size());
			for (Map.Entry<String, Roaring64Bitmap> en : map.entrySet()) {
				dos.writeUTF(en.getKey());
				Roaring64Bitmap value = en.getValue();
				value.runOptimize();
				value.writeExternal(dos);
			}

		} catch (IOException e) {
			log.error("IO issue", e);
		}
	}

	public static Set<String> findAllNonVirtuosoGraphs(RepositoryConnection connection, AtomicInteger scheduledQueries2,
			AtomicInteger finishedQueries2) throws RepositoryException {
		Set<String> res = new HashSet<>();

		scheduledQueries2.incrementAndGet();
		final TupleQuery graphs = connection.prepareTupleQuery(QueryLanguage.SPARQL,
				"SELECT DISTINCT ?g WHERE {GRAPH ?g { ?s ?p ?o}}");
		try (final TupleQueryResult foundGraphs = graphs.evaluate()) {
			while (foundGraphs.hasNext()) {
				final BindingSet next = foundGraphs.next();
				final String graphIRI = next.getBinding("g").getValue().stringValue();
				if (!VIRTUOSO_GRAPHS.contains(graphIRI))
					res.add(graphIRI);
			}
		} finally {
			finishedQueries2.incrementAndGet();
		}
		return res;

	}

	private void saveResults(IRI graphUri, Consumer<ServiceDescription> saver) {
		sd.setTotalTripleCount(sd.getGraphs().stream().mapToLong(GraphDescription::getTripleCount).sum());
		saver.accept(sd);
		if (add) {
			try (RepositoryConnection connection = repository.getConnection()) {
				clearGraph(connection, graphUri);
				updateGraph(connection, graphUri);
			} catch (RepositoryException | MalformedQueryException | IOException e) {
				log.error("Generating stats for SD failed", e);
			}
		}
	}

	private void countTheVoidDataItself(ExecutorService executors, List<Future<Exception>> futures, IRI voidGraph,
			Consumer<ServiceDescription> saver, ConcurrentHashMap<String, Roaring64Bitmap> distinctSubjectIris,
			ConcurrentHashMap<String, Roaring64Bitmap> distinctObjectIris, boolean isVirtuoso, Semaphore limit) {
		String voidGraphUri = voidGraph.toString();
		if (!graphNames.contains(voidGraphUri)) {
			scheduleBigCountsPerGraph(executors, sd, futures, voidGraphUri, saver, limit);
			Lock writeLock = rwLock.writeLock();
			if (isVirtuoso) {
				futures.add(executors.submit(new CountDistinctIriSubjectsInAGraphVirtuoso(sd, repository, saver,
						writeLock, distinctSubjectIris, voidGraphUri, limit, scheduledQueries, finishedQueries)));
				futures.add(executors.submit(new CountDistinctIriObjectsInAGraphVirtuoso(sd, repository, saver,
						writeLock, distinctObjectIris, voidGraphUri, limit, scheduledQueries, finishedQueries)));
			}
			countSpecificThingsPerGraph(executors, sd, knownPredicates, futures, voidGraphUri, limit, saver);
		}
	}

	private void waitForCountToFinish(List<Future<Exception>> futures) {
		INTERRUPTED: // We want to wait for all threads to finish even if interrupted waiting for the
						// results.
		try {

			while (!futures.isEmpty()) {
				log.info("Queries " + finishedQueries.get() + "/" + scheduledQueries.get());
				final int last = futures.size() - 1;
				final Future<Exception> next = futures.get(last);
				if (next.isDone() || next.isCancelled())
					futures.remove(last);
				else {
					try {
						Exception exception = next.get(1, TimeUnit.MINUTES);
						futures.remove(last);
						if (exception != null) {
							log.error("Something failed", exception);
						}
					} catch (CancellationException | ExecutionException e) {
						log.error("Counting subjects or objects failed", e);
					} catch (TimeoutException e) {
						// This is ok we just try again in the loop.
					}
				}
			}
		} catch (InterruptedException e) {
			// Clear interrupted flag
			Thread.interrupted();
			// Try again to get all of the results.
			break INTERRUPTED;
		}
		if (finishedQueries.get() == scheduledQueries.get()) {
			log.info("Ran " + finishedQueries.get() + " queries");
		} else {
			log.error("Scheduled more queries than finished: " + finishedQueries.get() + "/" + scheduledQueries.get());
		}
	}

	private List<Future<Exception>> scheduleCounters(ExecutorService executors, ServiceDescription sd,
			Consumer<ServiceDescription> saver, ConcurrentHashMap<String, Roaring64Bitmap> distinctSubjectIris,
			ConcurrentHashMap<String, Roaring64Bitmap> distinctObjectIris, Semaphore limit) {
		List<Future<Exception>> futures = Collections.synchronizedList(new ArrayList<>());
		Lock writeLock = rwLock.writeLock();
		boolean isvirtuoso = repository instanceof VirtuosoRepository;
		if (graphNames.isEmpty()) {
			try (RepositoryConnection connection = repository.getConnection()) {
				graphNames = findAllNonVirtuosoGraphs(connection, scheduledQueries, finishedQueries);
			}
		}
		if (countDistinctObjects) {
			countDistinctObjects(executors, sd, saver, distinctObjectIris, futures, writeLock, isvirtuoso, graphNames,
					limit);
		}

		if (countDistinctSubjects) {
			countDistinctSubjects(executors, sd, saver, distinctSubjectIris, futures, writeLock, isvirtuoso, graphNames,
					limit);
		}

		for (String graphName : graphNames) {
			scheduleBigCountsPerGraph(executors, sd, futures, graphName, saver, limit);
		}

		// Ensure that we first do the big counts before starting on counting the
		// smaller sets.
		for (String graphName : graphNames) {
			countSpecificThingsPerGraph(executors, sd, knownPredicates, futures, graphName, limit, saver);
		}
		return futures;
	}

	private void countDistinctObjects(ExecutorService executors, ServiceDescription sd,
			Consumer<ServiceDescription> saver, ConcurrentHashMap<String, Roaring64Bitmap> distinctObjectIris,
			List<Future<Exception>> futures, Lock writeLock, boolean isvirtuoso, Collection<String> allGraphs,
			Semaphore limit) {
		futures.add(executors.submit(new CountDistinctBnodeObjectsForAllGraphs(sd, repository, saver, writeLock, limit,
				scheduledQueries, finishedQueries)));
		if (!isvirtuoso) {
			futures.add(executors.submit(new CountDistinctIriObjectsForAllGraphsAtOnce(sd, repository, saver, writeLock,
					limit, scheduledQueries, finishedQueries)));
		} else {
			for (String graphName : allGraphs) {
				futures.add(executors.submit(new CountDistinctIriObjectsInAGraphVirtuoso(sd, repository, saver,
						writeLock, distinctObjectIris, graphName, limit, scheduledQueries, finishedQueries)));
			}
		}
		futures.add(executors.submit(new CountDistinctLiteralObjectsForAllGraphs(sd, repository, saver, writeLock,
				limit, scheduledQueries, finishedQueries)));
	}

	private void countDistinctSubjects(ExecutorService executors, ServiceDescription sd,
			Consumer<ServiceDescription> saver, ConcurrentHashMap<String, Roaring64Bitmap> distinctSubjectIris,
			List<Future<Exception>> futures, Lock writeLock, boolean isvirtuoso, Collection<String> allGraphs,
			Semaphore limit) {
		if (!isvirtuoso) {
			futures.add(executors.submit(new CountDistinctIriSubjectsForAllGraphs(sd, repository, saver, writeLock,
					limit, scheduledQueries, finishedQueries)));
		} else {
			for (String graphName : allGraphs) {
				futures.add(executors.submit(new CountDistinctIriSubjectsInAGraphVirtuoso(sd, repository, saver,
						writeLock, distinctSubjectIris, graphName, limit, scheduledQueries, finishedQueries)));
			}
		}
		futures.add(executors.submit(
				new CountDistinctBnodeSubjects(sd, repository, writeLock, limit, scheduledQueries, finishedQueries)));
	}

	private void countSpecificThingsPerGraph(ExecutorService executors, ServiceDescription sd, Set<IRI> knownPredicates,
			List<Future<Exception>> futures, String graphName, Semaphore limit, Consumer<ServiceDescription> saver) {
		final GraphDescription gd = getOrCreateGraphDescriptionObject(graphName, sd);
		if (countDistinctClasses && findPredicates && detailedCount) {
			futures.add(executors.submit(new FindPredicatesAndClasses(gd, repository, executors, futures,
					knownPredicates, rwLock, limit, scheduledQueries, finishedQueries, saver, sd)));
		} else {
			Lock writeLock = rwLock.writeLock();
			if (findPredicates) {
				futures.add(executors.submit(new FindPredicates(gd, repository, knownPredicates, futures, executors,
						writeLock, limit, scheduledQueries, finishedQueries, saver, sd)));
			}
			if (countDistinctClasses) {
				futures.add(executors.submit(new CountDistinctClassses(gd, repository, writeLock, limit,
						scheduledQueries, finishedQueries, saver, sd)));
			}
		}
	}

	private void scheduleBigCountsPerGraph(ExecutorService executors, ServiceDescription sd,
			List<Future<Exception>> futures, String graphName, Consumer<ServiceDescription> saver, Semaphore limit) {
		final GraphDescription gd = getOrCreateGraphDescriptionObject(graphName, sd);
		Lock writeLock = rwLock.writeLock();
		// Objects are hardest to count so schedules first.
		if (countDistinctObjects) {
			futures.add(executors.submit(new CountDistinctLiteralObjects(gd, sd, repository, saver, writeLock, limit,
					scheduledQueries, finishedQueries)));
		}
		if (countDistinctSubjects) {
			futures.add(executors.submit(new CountDistinctBnodeSubjects(gd, repository, writeLock, limit,
					scheduledQueries, finishedQueries)));
		}
		futures.add(
				executors.submit(new TripleCount(gd, repository, writeLock, limit, scheduledQueries, finishedQueries)));
	}

	protected GraphDescription getOrCreateGraphDescriptionObject(String graphName, ServiceDescription sd) {
		GraphDescription pgd = sd.getGraph(graphName);
		if (pgd == null) {
			pgd = new GraphDescription();
			pgd.setGraphName(graphName);
			Lock writeLock = rwLock.writeLock();
			try {
				writeLock.lock();
				sd.putGraphDescription(pgd);
			} finally {
				writeLock.unlock();
			}
		}
		return pgd;
	}

	private void updateGraph(RepositoryConnection connection, IRI voidGraphUri)
			throws RepositoryException, RDFParseException, IOException {
		log.debug("Updating " + voidGraphUri);

		try (InputStream in = new FileInputStream(sdFile)) {
			RDFXMLParser p = new RDFXMLParser();
			p.setRDFHandler(new RDFHandler() {

				@Override
				public void startRDF() throws RDFHandlerException {
					if (!connection.isActive())
						connection.begin();
				}

				@Override
				public void handleStatement(Statement st) throws RDFHandlerException {
					connection.add(st, voidGraphUri);

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
					connection.commit();
				}
			});
			p.parse(in);
		}

		log.debug("Updating " + voidGraphUri + " done");
	}

	private void clearGraph(RepositoryConnection connection, IRI voidGraphUri)
			throws RepositoryException, MalformedQueryException {

		connection.begin();
		final Update prepareUpdate = connection.prepareUpdate(QueryLanguage.SPARQL,
				"DROP SILENT GRAPH <" + voidGraphUri + ">");
		try {
			prepareUpdate.execute();
			connection.commit();
		} catch (UpdateExecutionException e1) {
			log.error("Clearing graph failed", e1);
			connection.rollback();
		}
	}

	public static Value getFirstResultFromTupleQuery(String sq, RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		try (TupleQueryResult classes = runTupleQuery(sq, connection)) {
			if (classes.hasNext()) {
				Binding types = classes.next().getBinding("types");
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

	private static final class FindPredicatesAndClasses implements Callable<Exception> {
		private final GraphDescription gd;
		private final Repository repository;
		private final ExecutorService execs;
		private final List<Future<Exception>> futures;
		private final Set<IRI> knownPredicates;
		private final ReadWriteLock rwLock;
		private final Semaphore limit;
		private final AtomicInteger scheduledQueries;
		private final AtomicInteger finishedQueries;
		private final Consumer<ServiceDescription> saver;
		private final ServiceDescription sd;

		private FindPredicatesAndClasses(GraphDescription gd, Repository repository, ExecutorService execs,
				List<Future<Exception>> futures, Set<IRI> knownPredicates, ReadWriteLock rwLock, Semaphore limit,
				AtomicInteger scheduledQueries, AtomicInteger finishedQueries, Consumer<ServiceDescription> saver,
				ServiceDescription sd) {
			this.gd = gd;
			this.repository = repository;
			this.execs = execs;
			this.futures = futures;
			this.knownPredicates = knownPredicates;
			this.rwLock = rwLock;
			this.limit = limit;
			this.scheduledQueries = scheduledQueries;
			this.finishedQueries = finishedQueries;
			this.saver = saver;
			this.sd = sd;
		}

		@Override
		public Exception call() {
			final Lock writeLock = rwLock.writeLock();
			Exception call = new FindPredicates(gd, repository, knownPredicates, futures, execs, writeLock, limit,
					scheduledQueries, finishedQueries, saver, sd).call();
			if (call != null)
				return call;

			call = new CountDistinctClassses(gd, repository, writeLock, limit, scheduledQueries, finishedQueries, saver,
					sd).call();
			if (call != null)
				return call;

			Set<ClassPartition> classes;
			Set<PredicatePartition> predicates;

			final Lock readLock = rwLock.readLock();
			try {
				readLock.lock();
				classes = new HashSet<>(gd.getClasses());
				predicates = new HashSet<>(gd.getPredicates());
			} finally {
				readLock.unlock();
			}

			for (PredicatePartition predicate : predicates) {
				for (ClassPartition source : classes) {
					if (!RDF.TYPE.equals(predicate.getPredicate()))
						futures.add(execs.submit(new FindPredicateLinkSets(repository, classes, predicate, source,
								writeLock, futures, limit, execs, gd, scheduledQueries, finishedQueries, saver, sd)));
				}
			}
			return null;
		}

	}

	public static long getSingleLongFromSql(String sql, VirtuosoRepositoryConnection connection) {
		Connection vrc = connection.getQuadStoreConnection();
		try (java.sql.Statement stat = vrc.createStatement()) {
			stat.setFetchSize(1);
			try (ResultSet rs = stat.executeQuery(sql)) {
				if (rs.next())
					return rs.getLong(1);
				else {
					log.debug("FAILED to get result for:" + sql);
					throw new RepositoryException("FAILED to get result for:" + sql);
				}
			}
		} catch (SQLException e) {
			log.debug("FAILED connection:" + sql);
			throw new RepositoryException(e);
		}
	}
}
