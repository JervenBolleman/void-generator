package swiss.sib.swissprot.servicedescription.io;

import java.time.LocalDate;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.xml.datatype.XMLGregorianCalendar;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.DCTERMS;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.servicedescription.ServiceDescription;
import swiss.sib.swissprot.vocabulary.PAV;
import swiss.sib.swissprot.vocabulary.VOID_EXT;

public class ServiceDescriptionRDFReader {
	private static final Logger log = LoggerFactory.getLogger(ServiceDescriptionRDFReader.class);
	public static final ValueFactory VF = SimpleValueFactory.getInstance();

	public static ServiceDescription read(Model source) {
		ServiceDescription sd = new ServiceDescription();

		Resource serviceIri = getOne(source, null, RDF.TYPE, SD.SERVICE, Statement::getSubject);

		IRI endpoint = (IRI) getOne(source, serviceIri, SD.ENDPOINT, null, Statement::getObject);
		
		sd.setEndpoint(endpoint);
		
		Resource defaultDataset = (Resource) getOne(source, serviceIri, SD.DEFAULT_DATASET, null, Statement::getObject);

		Resource defaultGraph = (Resource) getOne(source, defaultDataset, SD.DEFAULT_GRAPH, null, Statement::getObject);

		getAndSetOne(source, defaultDataset, DCTERMS.ISSUED, null, Statement::getObject, (o) -> {
			XMLGregorianCalendar defaultDatasetIssue = ((Literal) o).calendarValue();
			sd.setReleaseDate(LocalDate.of(defaultDatasetIssue.getYear(), defaultDatasetIssue.getMonth(),
					defaultDatasetIssue.getDay()));
		});
		getAndSetOne(source, defaultDataset, PAV.VERSION, null, Statement::getObject,
				(o) -> sd.setVersion(o.stringValue()));
		getAndSetOne(source, defaultGraph, DCTERMS.TITLE, null, Statement::getObject,
				(o) -> sd.setTitle(o.stringValue()));

		Iterator<Statement> namedGraphs = source.getStatements(defaultDataset, SD.NAMED_GRAPH_PROPERTY, null)
				.iterator();
		while (namedGraphs.hasNext()) {
			IRI namedGraph = (IRI) namedGraphs.next().getObject();
			GraphDescription gd = new GraphDescription();
			gd.setGraph(namedGraph);
			sd.putGraphDescription(gd);
			getAndSetOne(source, namedGraph, DCTERMS.LICENSE, null, Statement::getObject,
					o -> gd.setLicense((IRI) o));
			
			IRI graphIri = (IRI) getOne(source, namedGraph, SD.GRAPH_PROPERTY, null, Statement::getObject);
			getAndSetOne(source, namedGraph, SD.NAME, null, Statement::getObject,
					o -> gd.setGraphName(o.stringValue()));
			getAndSetOne(source, graphIri, VOID.TRIPLES, null, Statement::getObject,
					o -> gd.setTripleCount(asLong(o)));
			getAndSetOne(source, graphIri, DCTERMS.LICENSE, null, Statement::getObject,
					o -> gd.setLicense((IRI) o));
			getAndSetOne(source, graphIri, VOID_EXT.DISTINCT_BLANK_NODE_OBJECTS, null, Statement::getObject,
					o -> gd.setDistinctBnodeObjectCount(asLong(o)));
			getAndSetOne(source, graphIri, VOID_EXT.DISTINCT_IRI_REFERENCE_OBJECTS, null, Statement::getObject,
					o -> gd.setDistinctIriObjectCount(asLong(o)));
			getAndSetOne(source, graphIri, VOID_EXT.DISTINCT_LITERALS, null, Statement::getObject,
					o -> gd.setDistinctLiteralObjectCount(asLong(o)));
			getAndSetOne(source, graphIri, VOID_EXT.DISTINCT_BLANK_NODE_SUBJECTS, null, Statement::getObject,
					o -> gd.setDistinctBnodeSubjectCount(asLong(o)));
			getAndSetOne(source, graphIri, VOID_EXT.DISTINCT_IRI_REFERENCE_SUBJECTS, null, Statement::getObject,
					o -> gd.setDistinctIriSubjectCount(asLong(o)));
	
			Iterator<Statement> predicates = source.getStatements(graphIri, VOID.PROPERTY_PARTITION, null).iterator();
			Set<PredicatePartition> predicates2 = gd.getPredicates();
			handlePredicatePartition(source, predicates, predicates2, gd.getPredicates()::add);

			Iterator<Statement> classPartitions = source.getStatements(graphIri, VOID.CLASS_PARTITION, null)
					.iterator();
			while (classPartitions.hasNext()) {
				Resource classPartition = (Resource) classPartitions.next().getObject();
				IRI clazzIri = (IRI) getOne(source, classPartition, VOID.CLASS, null, Statement::getObject);
				ClassPartition cp = new ClassPartition(clazzIri);
				gd.getClasses().add(cp);
				Iterator<Statement> cpPredicates = source.getStatements(graphIri, VOID.PROPERTY_PARTITION, null)
						.iterator();
				handlePredicatePartition(source, cpPredicates, cp.getPredicatePartitions(), cp::putPredicatePartition);
			}
		}
		return sd;
	}

	private static void handlePredicatePartition(Model source, Iterator<Statement> predicates,
			Collection<PredicatePartition> predicates2, Consumer<PredicatePartition> cp) {
		while (predicates.hasNext()) {
			Statement next = predicates.next();
			IRI predicate = (IRI) next.getObject();
			PredicatePartition pp = new PredicatePartition();
			pp.setPredicate(predicate);
			cp.accept(pp);
			getAndSetOne(source, predicate, VOID.TRIPLES, null, Statement::getObject,
					o -> pp.setTripleCount(asLong(o)));
			getAndSetOne(source, predicate, VOID.DISTINCT_OBJECTS, null, Statement::getObject,
					o -> pp.setDistinctObjectCount(asLong(o)));
			getAndSetOne(source, predicate, VOID.DISTINCT_SUBJECTS, null, Statement::getObject,
					o -> pp.setDistinctSubjectCount(asLong(o)));
		}
	}

	/**
	 * Get one result from the model for a triple specification.
	 * 
	 * 
	 * @param <T> The resulting type of the data extracted from the statement
	 * @param source model to extract data from
	 * @param subject may be null
	 * @param predicate may be null
	 * @param object may be null
	 * @param extract the function to extract anything from a statement
	 * @return the desired value or null
	 */
	private static <T extends Value> T getOne(Model source, Resource subject, IRI predicate, Value object,
			Function<Statement, T> extract) {
		Iterator<Statement> iter = source.getStatements(subject, predicate, object).iterator();
		if (iter.hasNext()) {
			Statement statement = iter.next();
			return extract.apply(statement);
		}
		return null;
	}

	/**
	 * Take a value from a statement, convert to a value T and if not null pass it to an accepting consumer.
	 * @param <T> The resulting type of the data extracted from the statement
	 * @param source model to extract data from
	 * @param subject may be null
	 * @param predicate may be null
	 * @param object may be null
	 * @param extract a value from the statement and convert
	 * @param setter take a <T> and if not null accept it
	 */
	private static <T extends Value> void getAndSetOne(Model source, Resource subject, IRI predicate, Value object,
			Function<Statement, T> extract, Consumer<T> setter) {
		Iterator<Statement> iter = source.getStatements(subject, predicate, object).iterator();
		if (iter.hasNext()) {
			Statement statement = iter.next();
			T t = extract.apply(statement);
			if (t != null) {
				setter.accept(t);
			}
		}
	}

	/**
	 * Turn a value into a long, assumes this will be successful. 
	 * @param o a value
	 * @return an long
	 */
	private static Long asLong(Value o) {
		return ((Literal) o).longValue();
	}
}
