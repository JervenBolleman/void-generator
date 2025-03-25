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
import java.net.URI;
import java.net.URISyntaxException;
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

import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.http.impl.client.HttpClientBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
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

import com.inrupt.client.spi.RdfService;
import com.inrupt.client.solid.SolidRDFSource;
import com.inrupt.client.solid.SolidSyncClient;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import swiss.sib.swissprot.servicedescription.io.ServiceDescriptionRDFWriter;
import swiss.sib.swissprot.voidcounter.CountDistinctBnodeObjectsForAllGraphs;
import swiss.sib.swissprot.voidcounter.CountDistinctBnodeSubjects;
import swiss.sib.swissprot.voidcounter.FindDistinctClassses;
import swiss.sib.swissprot.voidcounter.CountDistinctIriObjectsForAllGraphsAtOnce;
import swiss.sib.swissprot.voidcounter.CountDistinctIriSubjectsForAllGraphs;
import swiss.sib.swissprot.voidcounter.CountDistinctLiteralObjects;
import swiss.sib.swissprot.voidcounter.CountDistinctLiteralObjectsForAllGraphs;
import swiss.sib.swissprot.voidcounter.FindPredicates;
import swiss.sib.swissprot.voidcounter.QueryCallable;
import swiss.sib.swissprot.voidcounter.TripleCount;
import swiss.sib.swissprot.voidcounter.virtuoso.CountDistinctIriSubjectsAndObjectsInAGraphVirtuoso;
import swiss.sib.swissprot.voidcounter.virtuoso.CountDistinctIriSubjectsInAGraphVirtuoso;
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

	@Option(names = { "-r", "--repository" }, description = "A SPARQL http/https endpoint location", required = true)
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

	@Option(names = { "--from-solid-pod" }, description = "generate a void/service description for a solid pod")
	private String solidPodIri;

	@Option(names = {
			"--filter-expression-to-exclude-classes-from-void" }, description = "Some classes are not interesting for the void file, as they are to rare. Can occur if many classes have instances but the classes do not represent a schema as such. Variable should be '?clazz'")
	private String classExclusion;

	@Option(names = {
			"--data-release-version" }, description = "Set a 'version' for the sparql-endpoint data and the datasets")
	private String dataVersion;

	@Option(names = {
			"--data-release-date" }, description = "Set a 'date' of release for the sparql-endpoint data and the datasets")
	private String dataReleaseDate;

	public static void main(String[] args) {
		int exitCode = new CommandLine(new Generate()).execute(args);
		System.exit(exitCode);
	}

	final AtomicInteger scheduledQueries = new AtomicInteger();
	final AtomicInteger finishedQueries = new AtomicInteger();

	private static final ValueFactory VF = SimpleValueFactory.getInstance();

	private static final Pattern COMMA = Pattern.compile(",", Pattern.LITERAL);

	@Override
	public Integer call() throws Exception {
		if (commaSeperatedGraphs != null)
			this.graphNames = COMMA.splitAsStream(commaSeperatedGraphs).collect(Collectors.toSet());
		else
			this.graphNames = new HashSet<>();

		log.debug("Void listener for " + graphNames.stream().collect(Collectors.joining(", ")));
		if (commaSeperatedKnownPredicates != null) {
			this.knownPredicates = COMMA.splitAsStream(commaSeperatedKnownPredicates).map(s -> VF.createIRI(s))
					.collect(Collectors.toSet());
		} else
			this.knownPredicates = new HashSet<>();
		if (repository == null) {
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
			} else if (solidPodIri != null) {
				loadFromSolidPod();
			} else if (repositoryLocator.startsWith("http")) {
				SPARQLRepository sr = new SPARQLRepository(repositoryLocator);
				sr.enableQuadMode(true);
				sr.setAdditionalHttpHeaders(Map.of("User-Agent", "void-generator"));
				HttpClientBuilder hcb = HttpClientBuilder.create();
				hcb.setMaxConnPerRoute(maxConcurrency).setMaxConnTotal(maxConcurrency)
						.setUserAgent("void-generator-robot");
				sr.setHttpClient(hcb.build());
				repository = sr;
			}
		}
		update();
		// Virtuoso was not started by us so we should not send it a shutdown command
		if (virtuosoJdcb == null) {
			repository.shutDown();
		}
		return 0;
	}

	private void loadFromSolidPod() throws URISyntaxException {
		SolidSyncClient client = SolidSyncClient.getClient();
		
		try (SolidRDFSource s = client.read(new URI(solidPodIri), SolidRDFSource.class)) {
			
			MemoryStore ms = new MemoryStore();
			ms.init();

			repository = new SailRepository(ms);
			repository.init();
			try (RepositoryConnection conn = repository.getConnection()) {
				conn.begin();
				Iterable<Quad> iter = s.iterate();
				int i = 0;
				for (Quad q : iter) {
					i++;
					Resource subj = toResource(q.getSubject());
					IRI pred = (IRI) toResource(q.getPredicate());
					Value object = toValue(q.getObject());
					Statement stat = VF.createStatement(subj, pred, object);
					if (q.getGraphName().isPresent()) {
						IRI graph = (IRI) toResource(q.getGraphName().get());
						conn.add(stat, graph);
					} else {
						//TODO: issue 28.
						IRI graph = VF.createIRI(solidPodIri);
						conn.add(stat, graph);
					}
					log.info("added {}", stat);
				}
				conn.commit();
				log.info("Added {} triples from the SolidPod:{}",i , solidPodIri);
			}
		}
	}

	private Value toValue(RDFTerm object) {
		if (object instanceof org.apache.commons.rdf.api.IRI si) {
			String st = si.getIRIString();
			return VF.createIRI(st);
		} else if (object instanceof org.apache.commons.rdf.api.BlankNode bn) {
			return VF.createBNode(bn.uniqueReference());
		} else if (object instanceof org.apache.commons.rdf.api.Literal l) {
			String ls = l.getLexicalForm();
			if (l.getDatatype() != null) {
				return VF.createLiteral(ls, VF.createIRI(l.getDatatype().getIRIString()));
			} else if (l.getLanguageTag().isPresent()) {
				return VF.createLiteral(ls, l.getLanguageTag().get());
			} else {
				return VF.createLiteral(ls);
			}
		}
		throw new IllegalStateException("Conversion between commons api and rdf4j api failed");
	}

	private Resource toResource(BlankNodeOrIRI subject) {
		if (subject instanceof org.apache.commons.rdf.api.IRI si) {
			String st = si.getIRIString();
			return VF.createIRI(st);
		} else if (subject instanceof org.apache.commons.rdf.api.BlankNode bn) {
			return VF.createBNode(bn.uniqueReference());
		}
		throw new RuntimeException("resource neither IRI nor BlankNode");
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

	void setSolidPodIri(String solidPodIri) {
		this.solidPodIri = solidPodIri;
	}

	private final List<Future<Exception>> futures = Collections.synchronizedList(new ArrayList<>());
	private final List<QueryCallable<?>> tasks = Collections.synchronizedList(new ArrayList<>());

	private final ExecutorService executors;

	private final Semaphore limit;

	public final CompletableFuture<Exception> schedule(QueryCallable<?> task) {
		scheduledQueries.incrementAndGet();
		tasks.add(task);
		CompletableFuture<Exception> cf = CompletableFuture.supplyAsync(() -> {
			return task.call();
		}, executors);

		futures.add(cf);
		return cf;
	}

	public void update() {
		log.debug("Void listener for " + graphNames.stream().collect(Collectors.joining(", ")));
		if (dataReleaseDate != null) {
			sd.setReleaseDate(LocalDate.from(DateTimeFormatter.ISO_DATE.parse(dataReleaseDate)));
		}
		sd.setVersion(dataVersion);
		if (commaSeperatedKnownPredicates != null) {
			this.knownPredicates = COMMA.splitAsStream(commaSeperatedKnownPredicates).map(s -> VF.createIRI(s))
					.collect(Collectors.toSet());
		} else
			this.knownPredicates = new HashSet<>();
		this.iriOfVoid = SimpleValueFactory.getInstance().createIRI(iriOfVoidAsString);
		this.distinctSubjectIrisFile = new File(sdFile.getParentFile(), sdFile.getName() + "subject-bitsets-per-graph");
		this.distinctObjectIrisFile = new File(sdFile.getParentFile(), sdFile.getName() + "object-bitsets-per-graph");

		ConcurrentHashMap<String, Roaring64NavigableMap> distinctSubjectIris = readGraphsWithSerializedBitMaps(
				this.distinctSubjectIrisFile);
		ConcurrentHashMap<String, Roaring64NavigableMap> distinctObjectIris = readGraphsWithSerializedBitMaps(
				this.distinctObjectIrisFile);
		Consumer<ServiceDescription> saver = (sdg) -> writeServiceDescriptionAndGraphs(distinctSubjectIris,
				distinctObjectIris, sdg, iriOfVoid);

		scheduleCounters(sd, saver, distinctSubjectIris, distinctObjectIris);

		waitForCountToFinish(futures);
		if (add) {
			log.debug("Starting the count of the void data itself using " + maxConcurrency);
			countTheVoidDataItself(iriOfVoid, saver, distinctSubjectIris, distinctObjectIris,
					repository instanceof VirtuosoRepository, limit);
			waitForCountToFinish(futures);
			saveResults(iriOfVoid, saver);

			log.debug("Starting the count of the void data itself a second time using " + maxConcurrency);

			countTheVoidDataItself(iriOfVoid, saver, distinctSubjectIris, distinctObjectIris,
					repository instanceof VirtuosoRepository, limit);
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
				log.error("", e);
			}
		}
		return map;
	}

	private void writeServiceDescriptionAndGraphs(ConcurrentHashMap<String, Roaring64NavigableMap> distinctSubjectIris,
			ConcurrentHashMap<String, Roaring64NavigableMap> distinctObjectIris, ServiceDescription sdg,
			IRI iriOfVoid) {
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
			if (virtuosoJdcb != null) {
				writeGraphsWithSerializedBitMaps(distinctSubjectIrisFile, distinctSubjectIris);
				writeGraphsWithSerializedBitMaps(distinctObjectIrisFile, distinctObjectIris);
			}
		} finally {
			readLock.unlock();
		}
	}

	private void writeGraphsWithSerializedBitMaps(File targetFile,
			ConcurrentHashMap<String, Roaring64NavigableMap> map) {
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

	private void countTheVoidDataItself(IRI voidGraph, Consumer<ServiceDescription> saver,
			ConcurrentHashMap<String, Roaring64NavigableMap> distinctSubjectIris,
			ConcurrentHashMap<String, Roaring64NavigableMap> distinctObjectIris, boolean isVirtuoso, Semaphore limit) {
		String voidGraphUri = voidGraph.toString();
		if (!graphNames.contains(voidGraphUri)) {
			scheduleBigCountsPerGraph(sd, voidGraphUri, saver, limit);
			Lock writeLock = rwLock.writeLock();
			if (isVirtuoso) {
				CountDistinctIriSubjectsAndObjectsInAGraphVirtuoso cdso = new CountDistinctIriSubjectsAndObjectsInAGraphVirtuoso(
						sd, repository, saver, writeLock, distinctSubjectIris, distinctObjectIris, voidGraphUri, limit,
						finishedQueries);
				schedule(cdso);
			}
			countSpecificThingsPerGraph(sd, knownPredicates, voidGraphUri, limit, saver);
		}
	}

	private void waitForCountToFinish(List<Future<Exception>> futures) {
		INTERRUPTED: // We want to wait for all threads to finish even if interrupted waiting for the
						// results.
		try {
			int loop = 0;
			while (!futures.isEmpty()) {
				final int last = futures.size() - 1;
				final Future<Exception> next = futures.get(last);
				if (next.isCancelled())
					log.error("" + next + " was cancelled");
				else if (next.isDone())
					futures.remove(last);
				else {
					log.info("Queries " + finishedQueries.get() + "/" + scheduledQueries.get());
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
						new ArrayList<>(tasks).stream().filter(QueryCallable::isRunning).forEach(t -> {
							log.info("Running: " + t.getClass() + " -> " + t.getQuery());
						});
						loop = 0;
					}
					loop++;
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
		} else if (finishedQueries.get() > scheduledQueries.get()) {
			log.error("Scheduled more queries than finished: " + finishedQueries.get() + "/" + scheduledQueries.get());
		} else {
			log.error("Finished more queries than scheduled: " + finishedQueries.get() + "/" + scheduledQueries.get());
		}
	}

	private void scheduleCounters(ServiceDescription sd, Consumer<ServiceDescription> saver,
			ConcurrentHashMap<String, Roaring64NavigableMap> distinctSubjectIris,
			ConcurrentHashMap<String, Roaring64NavigableMap> distinctObjectIris) {
		Lock writeLock = rwLock.writeLock();
		boolean isvirtuoso = repository instanceof VirtuosoRepository;
		if (graphNames.isEmpty()) {
			try (RepositoryConnection connection = repository.getConnection()) {
				graphNames = FindGraphs.findAllNonVirtuosoGraphs(connection, scheduledQueries, finishedQueries);
			}
		}
		// Ensure that the graph description exists so that we won't have an issue
		// accessing them at any point.
		for (var graphName : graphNames) {
			getOrCreateGraphDescriptionObject(graphName, sd);
		}

		if (countDistinctObjects && countDistinctSubjects && isvirtuoso) {
			for (String graphName : graphNames) {
				schedule(new CountDistinctIriSubjectsAndObjectsInAGraphVirtuoso(sd, repository, saver, writeLock,
						distinctSubjectIris, distinctObjectIris, graphName, limit, finishedQueries));
			}
		} else {
			if (countDistinctObjects) {
				countDistinctObjects(sd, saver, distinctObjectIris, writeLock, isvirtuoso, graphNames, limit);
			}

			if (countDistinctSubjects) {
				countDistinctSubjects(sd, saver, distinctSubjectIris, writeLock, isvirtuoso, graphNames, limit);
			}
		}
		for (String graphName : graphNames) {
			scheduleBigCountsPerGraph(sd, graphName, saver, limit);
		}

		// Ensure that we first do the big counts before starting on counting the
		// smaller sets.
		for (String graphName : graphNames) {
			countSpecificThingsPerGraph(sd, knownPredicates, graphName, limit, saver);
		}
	}

	private void countDistinctObjects(ServiceDescription sd, Consumer<ServiceDescription> saver,
			ConcurrentHashMap<String, Roaring64NavigableMap> distinctObjectIris, Lock writeLock, boolean isvirtuoso,
			Collection<String> allGraphs, Semaphore limit) {
		schedule(new CountDistinctBnodeObjectsForAllGraphs(sd, repository, saver, writeLock, limit, finishedQueries));
		if (!isvirtuoso) {
			schedule(new CountDistinctIriObjectsForAllGraphsAtOnce(sd, repository, saver, writeLock, limit,
					finishedQueries));
		} else if (!countDistinctSubjects) {
			schedule(new CountDistinctIriObjectsForAllGraphsAtOnce(sd, repository, saver, writeLock, limit,
					finishedQueries));
		}
		schedule(new CountDistinctLiteralObjectsForAllGraphs(sd, repository, saver, writeLock, limit, finishedQueries));
	}

	private void countDistinctSubjects(ServiceDescription sd, Consumer<ServiceDescription> saver,
			ConcurrentHashMap<String, Roaring64NavigableMap> distinctSubjectIris, Lock writeLock, boolean isvirtuoso,
			Collection<String> allGraphs, Semaphore limit) {
		if (!isvirtuoso) {
			schedule(
					new CountDistinctIriSubjectsForAllGraphs(sd, repository, saver, writeLock, limit, finishedQueries));
		} else if (!countDistinctObjects) {
			for (String graphName : allGraphs) {
				schedule(new CountDistinctIriSubjectsInAGraphVirtuoso(sd, repository, saver, writeLock,
						distinctSubjectIris, graphName, limit, finishedQueries));
			}
		}
		schedule(new CountDistinctBnodeSubjects(sd, repository, writeLock, limit, finishedQueries, saver));
	}

	private void countSpecificThingsPerGraph(ServiceDescription sd, Set<IRI> knownPredicates, String graphName,
			Semaphore limit, Consumer<ServiceDescription> saver) {
		final GraphDescription gd = getOrCreateGraphDescriptionObject(graphName, sd);
		if (findDistinctClasses && findPredicates && detailedCount) {
			schedule(new FindPredicatesAndClasses(gd, repository, this::schedule, knownPredicates, rwLock, limit,
					finishedQueries, saver, sd, classExclusion));
		} else {
			Lock writeLock = rwLock.writeLock();
			if (findPredicates) {
				schedule(new FindPredicates(gd, repository, knownPredicates, this::schedule, writeLock, limit,
						finishedQueries, saver, sd, null));
			}
			if (findDistinctClasses) {
				schedule(new FindDistinctClassses(gd, repository, writeLock, limit, finishedQueries, saver,
						this::schedule, sd, classExclusion, null));
			}
		}
	}

	private void scheduleBigCountsPerGraph(ServiceDescription sd, String graphName, Consumer<ServiceDescription> saver,
			Semaphore limit) {
		final GraphDescription gd = getOrCreateGraphDescriptionObject(graphName, sd);
		Lock writeLock = rwLock.writeLock();
		// Objects are hardest to count so schedules first.
		if (countDistinctObjects) {
			schedule(new CountDistinctLiteralObjects(gd, sd, repository, saver, writeLock, limit, finishedQueries));
		}
		if (countDistinctSubjects) {
			schedule(new CountDistinctBnodeSubjects(gd, sd, repository, writeLock, limit, finishedQueries, saver));
		}
		schedule(new TripleCount(gd, repository, writeLock, limit, finishedQueries, saver, sd));
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
					connection.setNamespace(prefix, uri);
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
}
