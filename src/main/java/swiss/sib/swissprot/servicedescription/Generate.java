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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
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
import java.util.concurrent.CompletableFuture;
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

import org.apache.http.impl.client.HttpClientBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.rdfxml.RDFXMLParser;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import swiss.sib.swissprot.servicedescription.io.ServiceDescriptionRDFWriter;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.Counters;
import swiss.sib.swissprot.voidcounter.QueryCallable;
import swiss.sib.swissprot.voidcounter.sparql.SparqlCounters;
import swiss.sib.swissprot.voidcounter.virtuoso.VirtuosoCounters;
import virtuoso.rdf4j.driver.VirtuosoRepository;

@Command(name = "void-generate", mixinStandardHelpOptions = true, version = "0.1", description = "Generate a void file")
public class Generate implements Callable<Integer> {

	protected static final Logger log = LoggerFactory.getLogger(Generate.class);

	private Set<String> graphNames = Set.of();
	private Repository repository;

	private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
	private final ServiceDescription sd;

	@Option(names = "--find-distinct-classes", defaultValue = "true", description = "Setting this to false will disable the search for distinct classes. And also Linksets and limits most use cases")
	private boolean findDistinctClasses = true;
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

	@Option(names = { "--virtuoso-jdbc" }, description = "A virtuoso jdbc connection string")
	private String virtuosoJdcb;

	@Option(names = { "-r", "--repository" }, description = "A SPARQL http/https endpoint location")
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

	@Option(names = { "--add-void-graph-to-store",
			"-a" }, description = "immediatly attempt to store the void graph into the store and add it to the total count", defaultValue = "false")
	private boolean add = false;

	@Option(names = {
			"--max-concurrency", }, description = "issue no more than this number of queries at the same time")
	private int maxConcurrency = 0;

	@Option(names = { "--from-test-file" }, description = "generate a void/service description for a test file")
	private File fromTestFile;

	@Option(names = {
			"--filter-expression-to-exclude-classes-from-void" }, description = "Some classes are not interesting for the void file, as they are to rare. Can occur if many classes have instances but the classes do not represent a schema as such. Variable should be '?clazz'")
	private String classExclusion;

	@Option(names = {
			"--data-release-version" }, description = "Set a 'version' for the sparql-endpoint data and the datasets")
	private String dataVersion;

	@Option(names = {
			"--data-release-date" }, description = "Set a 'date' of release for the sparql-endpoint data and the datasets")
	private String dataReleaseDate;
	
	@Option(names = { "--optimize-for" }, description = "Which store to optimize for. Virtuoso, SPARQL or Qlever", defaultValue = "sparql")
	private String optimizeFor = "sparql";

	public static void main(String[] args) {
		int exitCode = new CommandLine(new Generate()).execute(args);
		System.exit(exitCode);
	}

	final AtomicInteger scheduledQueries = new AtomicInteger();
	final AtomicInteger finishedQueries = new AtomicInteger();

	private Counters counters;

	private static final ValueFactory VF = SimpleValueFactory.getInstance();

	private static final Pattern COMMA = Pattern.compile(",", Pattern.LITERAL);

	@Override
	public Integer call() throws Exception {
		if (commaSeperatedGraphs != null)
			this.graphNames = COMMA.splitAsStream(commaSeperatedGraphs).collect(Collectors.toSet());
		else
			this.graphNames = new HashSet<>();

		if (log.isDebugEnabled()) {
			log.debug("Void listener for {}", graphNames.stream().collect(Collectors.joining(", ")));
		}
		if (commaSeperatedKnownPredicates != null) {
			this.knownPredicates = COMMA.splitAsStream(commaSeperatedKnownPredicates).map(VF::createIRI)
					.collect(Collectors.toSet());
		} else
			this.knownPredicates = new HashSet<>();

		if (virtuosoJdcb != null) {
			repository = new VirtuosoRepository(virtuosoJdcb, user, password);
		} else if (fromTestFile != null) {
			MemoryStore ms = new MemoryStore();
			ms.init();

			repository = new SailRepository(ms);
			repository.init();
			try (RepositoryConnection conn = repository.getConnection()) {
				IRI graph = conn.getValueFactory().createIRI(fromTestFile.toURI().toString());
				conn.begin();
				conn.add(fromTestFile, graph);
				conn.commit();
			}
		} else if (repositoryLocator.startsWith("http")) {
			SPARQLRepository sr = new SPARQLRepository(repositoryLocator);
			sr.enableQuadMode(true);
			sr.setAdditionalHttpHeaders(Map.of("User-Agent", "void-generator"));
			HttpClientBuilder hcb = HttpClientBuilder.create();
			hcb.setMaxConnPerRoute(maxConcurrency).setMaxConnTotal(maxConcurrency).setUserAgent("void-generator-robot");
			sr.setHttpClient(hcb.build());
			repository = sr;
		}
		update();
		// Virtuoso was not started by us so we should not send it a shutdown command
		if (virtuosoJdcb == null) {
			repository.shutDown();
		}
		return 0;
	}

	public Generate() {
		super();
		this.sd = new ServiceDescription();
		// At least 1 but no more than one third of the cpus
		if (maxConcurrency <= 0) {
			maxConcurrency = Math.max(1, Runtime.getRuntime().availableProcessors() / 3);
		}
		limit = new Semaphore(maxConcurrency);
		executors = Executors.newFixedThreadPool(maxConcurrency);
	}

	private final List<Future<Exception>> futures = Collections.synchronizedList(new ArrayList<>());
	private final List<QueryCallable<?>> tasks = Collections.synchronizedList(new ArrayList<>());

	private final ExecutorService executors;

	private final Semaphore limit;

	public final CompletableFuture<Exception> schedule(QueryCallable<?> task) {
		scheduledQueries.incrementAndGet();
		tasks.add(task);
		CompletableFuture<Exception> cf = CompletableFuture.supplyAsync(task::call, executors);

		futures.add(cf);
		return cf;
	}

	public void update() {
		if (log.isDebugEnabled())
			log.debug("Void listener for {}", graphNames.stream().collect(Collectors.joining(", ")));
		if (dataReleaseDate != null) {
			sd.setReleaseDate(LocalDate.from(DateTimeFormatter.ISO_DATE.parse(dataReleaseDate)));
		}
		sd.setVersion(dataVersion);
		if (commaSeperatedKnownPredicates != null) {
			this.knownPredicates = COMMA.splitAsStream(commaSeperatedKnownPredicates).map(VF::createIRI)
					.collect(Collectors.toSet());
		} else
			this.knownPredicates = new HashSet<>();
		this.iriOfVoid = SimpleValueFactory.getInstance().createIRI(iriOfVoidAsString);
		Consumer<ServiceDescription> saver;
		if (repository instanceof VirtuosoRepository) {
			this.distinctSubjectIrisFile = new File(sdFile.getAbsolutePath() + ".distinct-subjects");
			this.distinctObjectIrisFile = new File(sdFile.getAbsolutePath() + ".distinct-objects");
			ConcurrentHashMap<String, Roaring64NavigableMap> distinctSubjectIris = readGraphsWithSerializedBitMaps(
					this.distinctSubjectIrisFile);
			ConcurrentHashMap<String, Roaring64NavigableMap> distinctObjectIris = readGraphsWithSerializedBitMaps(
					this.distinctObjectIrisFile);
			this.counters = new VirtuosoCounters(distinctSubjectIris, distinctObjectIris);
			saver = sdg -> writeServiceDescriptionAndGraphs(distinctSubjectIris, distinctObjectIris, sdg, iriOfVoid);
			optimizeFor = "virtuoso";
		} else {
			OptimizeFor fromString = OptimizeFor.fromString(optimizeFor);
			log.info("Optimizing for backing store: {}", fromString);
			this.counters = new SparqlCounters(fromString);
			saver = sdg -> writeServiceDescription(sdg, iriOfVoid);
		}

		scheduleCounters(sd, saver);

		waitForCountToFinish(futures);
		if (add) {
			log.debug("Starting the count of the void data itself using {}", maxConcurrency);
			countTheVoidDataItself(iriOfVoid, saver, limit);
			waitForCountToFinish(futures);
			saveResults(iriOfVoid, saver);

			log.debug("Starting the count of the void data itself a second time using {}", maxConcurrency);

			countTheVoidDataItself(iriOfVoid, saver, limit);
			waitForCountToFinish(futures);
		}
		saveResults(iriOfVoid, saver);
		executors.shutdown();
	}

	private ConcurrentHashMap<String, Roaring64NavigableMap> readGraphsWithSerializedBitMaps(File file) {
		ConcurrentHashMap<String, Roaring64NavigableMap> map = new ConcurrentHashMap<>();
		if (file.exists() && !forcedRefresh) {
			try (FileInputStream fis = new FileInputStream(file); ObjectInputStream bis = new ObjectInputStream(fis)) {
				int numberOfGraphs = bis.readInt();
				for (int i = 0; i < numberOfGraphs; i++) {
					String graph = bis.readUTF();
					Roaring64NavigableMap rb = new Roaring64NavigableMap();
					rb.readExternal(bis);
					map.put(graph, rb);
				}
			} catch (IOException e) {
				log.error("", e);
			} catch (ClassNotFoundException e) {
				log.error("Class can't be found code out of sync", e);
			}
		}
		return map;
	}

	private void writeServiceDescriptionAndGraphs(ConcurrentHashMap<String, Roaring64NavigableMap> distinctSubjectIris,
			ConcurrentHashMap<String, Roaring64NavigableMap> distinctObjectIris, ServiceDescription sdg,
			IRI iriOfVoid) {
		writeServiceDescription(sdg, iriOfVoid);
		writeGraphs(distinctSubjectIris, distinctObjectIris);
	}

	private void writeGraphs(ConcurrentHashMap<String, Roaring64NavigableMap> distinctSubjectIris,
			ConcurrentHashMap<String, Roaring64NavigableMap> distinctObjectIris) {
		final Lock readLock = rwLock.readLock();
		try {
			readLock.lock();

			if (virtuosoJdcb != null) {
				writeGraphsWithSerializedBitMaps(distinctSubjectIrisFile, distinctSubjectIris);
				writeGraphsWithSerializedBitMaps(distinctObjectIrisFile, distinctObjectIris);
			}
		} finally {
			readLock.unlock();
		}
	}

	private void writeServiceDescription(ServiceDescription sdg, IRI iriOfVoid) {
		final Lock readLock = rwLock.readLock();
		try {
			readLock.lock();
			Optional<RDFFormat> f = Rio.getWriterFormatForFileName(sdFile.getName());
			try (OutputStream os = new FileOutputStream(sdFile)) {
				ServiceDescriptionRDFWriter.write(sdg, iriOfVoid, f.orElseGet(() -> RDFFormat.TURTLE), os,
						VF.createIRI(repositoryLocator));
			} catch (Exception e) {
				log.error("can not store ServiceDescription", e);
			}
		} finally {
			readLock.unlock();
		}
	}

	private void writeGraphsWithSerializedBitMaps(File targetFile,
			ConcurrentHashMap<String, Roaring64NavigableMap> map) {
		if (!targetFile.exists()) {
			try {
				if (!targetFile.createNewFile()) {
					throw new RuntimeException("Can't create file we need later");
				}
			} catch (IOException e) {
				throw new RuntimeException("Can't create file we need later:", e);
			}
		}
		try (FileOutputStream fos = new FileOutputStream(targetFile);
				BufferedOutputStream bos = new BufferedOutputStream(fos);
				ObjectOutputStream dos = new ObjectOutputStream(bos)) {
			dos.writeInt(map.size());
			for (Map.Entry<String, Roaring64NavigableMap> en : map.entrySet()) {
				dos.writeUTF(en.getKey());
				Roaring64NavigableMap value = en.getValue();
				value.runOptimize();
				value.writeExternal(dos);
			}

		} catch (IOException e) {
			log.error("IO issue", e);
		}
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

	private void countTheVoidDataItself(IRI voidGraph, Consumer<ServiceDescription> saver, Semaphore limit) {
		String voidGraphUri = voidGraph.toString();
		GraphDescription voidGraphDescription = getOrCreateGraphDescriptionObject(voidGraphUri, sd);
		scheduleBigCountsPerGraph(sd, voidGraphDescription, saver, limit);
		Lock writeLock = rwLock.writeLock();
		if (repository instanceof VirtuosoRepository) {
			CommonVariables cv = new CommonVariables(sd, voidGraphDescription, repository, saver, writeLock, limit,
					finishedQueries);
			schedule(counters.countDistinctIriSubjectsAndObjectsInAGraph(cv));
		}
		countSpecificThingsPerGraph(sd, knownPredicates, voidGraphDescription, limit, saver);
	}

	private void waitForCountToFinish(List<Future<Exception>> futures) {
		try {
			int loop = 0;
			while (!futures.isEmpty()) {
				final int last = futures.size() - 1;
				final Future<Exception> next = futures.get(last);
				if (next.isCancelled())
					log.error("{} was cancelled", next);
				else if (next.isDone())
					futures.remove(last);
				else {
					log.info("Queries " + finishedQueries.get() + "/" + scheduledQueries.get());
					loop = checkProgress(futures, loop, last, next);
				}
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		if (finishedQueries.get() == scheduledQueries.get()) {
			log.info("Ran " + finishedQueries.get() + " queries");
		} else if (finishedQueries.get() > scheduledQueries.get()) {
			log.error("Scheduled more queries than finished: " + finishedQueries.get() + "/" + scheduledQueries.get());
		} else {
			log.error("Finished more queries than scheduled: " + finishedQueries.get() + "/" + scheduledQueries.get());
		}
	}

	private int checkProgress(List<Future<Exception>> futures, int loop, final int last, final Future<Exception> next)
			throws InterruptedException {
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
		if (loop == 60) {
			// We make a defensive copy of the tasks to avoid concurrent modification
			// exceptions
			new ArrayList<>(tasks).stream().filter(QueryCallable::isRunning)
					.forEach(t -> log.info("Running: " + t.getClass() + " -> " + t.getQuery()));
			loop = 0;
		}
		loop++;
		return loop;
	}

	private void scheduleCounters(ServiceDescription sd, Consumer<ServiceDescription> saver) {
		Lock writeLock = rwLock.writeLock();
		boolean optimizeForVirtuoso = "virtuoso".equalsIgnoreCase(optimizeFor);
		determineGraphNames(sd, saver, writeLock);
		
		Collection<GraphDescription> graphs = sd.getGraphs();

		if (countDistinctObjects && countDistinctSubjects && optimizeForVirtuoso) {
			for (GraphDescription gd : graphs) {
				CommonVariables cv = new CommonVariables(sd, gd, repository, saver, writeLock, limit, finishedQueries);
				schedule(counters.countDistinctIriSubjectsAndObjectsInAGraph(cv));
			}
		} else {
			CommonVariables cv = new CommonVariables(sd, null, repository, saver, writeLock, limit, finishedQueries);
			if (countDistinctObjects) {
				countDistinctObjects(sd, saver, writeLock, optimizeForVirtuoso, limit);
			}

			if (countDistinctSubjects) {
				countDistinctSubjects(cv, optimizeForVirtuoso, graphs);
			}
		}
		for (GraphDescription gd : graphs) {
			scheduleBigCountsPerGraph(sd, gd, saver, limit);
		}

		// Ensure that we first do the big counts before starting on counting the
		// smaller sets.
		for (GraphDescription gd : graphs) {
			countSpecificThingsPerGraph(sd, knownPredicates, gd, limit, saver);
		}
	}

	/**
	 * Ensure that the graph description exists so that we won't have an issue
	 * accessing them at any point.
	 * @param sd
	 * @param saver
	 * @param writeLock
	 */
	private void determineGraphNames(ServiceDescription sd, Consumer<ServiceDescription> saver, Lock writeLock) {
		if (graphNames.isEmpty()) {
			CommonVariables cv = new CommonVariables(sd, null, repository, saver, writeLock, limit, finishedQueries);
			QueryCallable<Set<String>> allGraphs = counters.findAllGraphs(cv);
			schedule(allGraphs);
			waitForCountToFinish(futures);
		} else {
			for (var graphName : graphNames) {
				getOrCreateGraphDescriptionObject(graphName, sd);
			}	
		}
	}

	private void countDistinctObjects(ServiceDescription sd, Consumer<ServiceDescription> saver, Lock writeLock,
			boolean isvirtuoso, Semaphore limit) {
		CommonVariables cv = new CommonVariables(sd, null, repository, saver, writeLock, limit, finishedQueries);
		schedule(counters.countDistinctBnodeObjectsInDefaultGraph(cv));
		if (!isvirtuoso || !countDistinctSubjects) {
			schedule(counters.countDistinctIriObjectsForDefaultGraph(cv));
		}
		schedule(counters.countDistinctLiteralObjectsForDefaultGraph(cv));
	}

	private void countDistinctSubjects(CommonVariables cv, boolean isvirtuoso, Collection<GraphDescription> allGraphs) {
		if (!isvirtuoso) {
			schedule(counters.countDistinctIriSubjectsForDefaultGraph(cv));
		} else if (!countDistinctObjects) {
			for (GraphDescription gd : allGraphs) {
				CommonVariables cvgd = new CommonVariables(cv.sd(), gd, cv.repository(), cv.saver(), cv.writeLock(),
						cv.limiter(), cv.finishedQueries());
				schedule(counters.countDistinctIriSubjectsInAGraph(cvgd));
			}
		}
		schedule(counters.countDistinctBnodeSubjectsInDefaultGraph(cv));
	}

	private void countSpecificThingsPerGraph(ServiceDescription sd, Set<IRI> knownPredicates, GraphDescription gd,
			Semaphore limit, Consumer<ServiceDescription> saver) {
		CommonVariables cv = new CommonVariables(sd, gd, repository, saver, rwLock.writeLock(), limit, finishedQueries);
		if (findDistinctClasses && findPredicates && detailedCount) {
			schedule(counters.findPredicatesAndClasses(cv, this::schedule, knownPredicates, rwLock, classExclusion));
		} else {
			if (findPredicates) {
				schedule(counters.findPredicates(cv, knownPredicates, this::schedule));
			}
			if (findDistinctClasses) {
				schedule(counters.findDistinctClassses(cv, this::schedule, classExclusion));
			}
		}
	}

	private void scheduleBigCountsPerGraph(ServiceDescription sd, GraphDescription gd,
			Consumer<ServiceDescription> saver, Semaphore limit) {
		Lock writeLock = rwLock.writeLock();
		CommonVariables cv = new CommonVariables(sd, gd, repository, saver, writeLock, limit, finishedQueries);
		// Objects are hardest to count so schedules first.
		if (countDistinctObjects) {
			schedule(counters.countDistinctLiteralObjects(cv));
		}
		if (countDistinctSubjects) {
			schedule(counters.countDistinctBnodeSubjects(cv));
		}
		schedule(counters.triples(cv));
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
		log.debug("Updating {}", voidGraphUri);

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
					connection.setNamespace(prefix, uri);
				}

				@Override
				public void handleComment(String comment) throws RDFHandlerException {
					// Ignore comments
				}

				@Override
				public void endRDF() throws RDFHandlerException {
					connection.commit();
				}
			});
			p.parse(in);
		}

		log.debug("Updating {} done", voidGraphUri);
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

	public Repository getRepository() {
		return repository;
	}

	public void setRepository(Repository repository) {
		this.repository = repository;
	}

	public void setGraphNames(Set<String> graphNames) {
		this.graphNames = graphNames;
	}

	public void setCountDistinctClasses(boolean countDistinctClasses) {
		this.findDistinctClasses = countDistinctClasses;
	}

	public void setCountDistinctSubjects(boolean countDistinctSubjects) {
		this.countDistinctSubjects = countDistinctSubjects;
	}

	public void setCountDistinctObjects(boolean countDistinctObjects) {
		this.countDistinctObjects = countDistinctObjects;
	}

	public void setFindPredicates(boolean findPredicates) {
		this.findPredicates = findPredicates;
	}

	public void setDetailedCount(boolean detailedCount) {
		this.detailedCount = detailedCount;
	}

	public void setOntologyGraphNames(List<String> ontologyGraphNames) {
		this.ontologyGraphNames = ontologyGraphNames;
	}

	public void setSdFile(File sdFile) {
		this.sdFile = sdFile;
	}

	public void setDistinctSubjectIrisFile(File distinctSubjectIrisFile) {
		this.distinctSubjectIrisFile = distinctSubjectIrisFile;
	}

	public void setDistinctObjectIrisFile(File distinctObjectIrisFile) {
		this.distinctObjectIrisFile = distinctObjectIrisFile;
	}

	public void setForcedRefresh(boolean forcedRefresh) {
		this.forcedRefresh = forcedRefresh;
	}

	public void setIriOfVoidAsString(String iriOfVoidAsString) {
		this.iriOfVoidAsString = iriOfVoidAsString;
	}

	public void setIriOfVoid(IRI iriOfVoid) {
		this.iriOfVoid = iriOfVoid;
		this.iriOfVoidAsString = iriOfVoid.stringValue();
	}

	public void setKnownPredicates(Set<IRI> knownPredicates) {
		this.knownPredicates = knownPredicates;
	}

	public void setCommaSeperatedGraphs(String commaSeperatedGraphs) {
		this.commaSeperatedGraphs = commaSeperatedGraphs;
	}

	public void setCommaSeperatedKnownPredicates(String commaSeperatedKnownPredicates) {
		this.commaSeperatedKnownPredicates = commaSeperatedKnownPredicates;
	}

	public void setAddResultsToStore(boolean add) {
		this.add = add;
	}

	/**
	 * The IRI of the endpoint
	 * 
	 * @param rl
	 */
	public void setRepositoryLocator(String rl) {
		this.repositoryLocator = rl;
	}

	public void setDataVersion(String dataVersion) {
		this.dataVersion = dataVersion;
	}

	public void setDataReleaseDate(String dataReleaseDate) {
		this.dataReleaseDate = dataReleaseDate;
	}

	public void setOptimizeFor(String optimizeFor) {
		this.optimizeFor = optimizeFor;
	}
}
