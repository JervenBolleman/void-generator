package swiss.sib.swissprot.servicedescription;

import java.time.LocalDate;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.DCTERMS;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.rio.RDFHandler;

import swiss.sib.swissprot.vocabulary.FORMATS;
import swiss.sib.swissprot.vocabulary.PAV;
import swiss.sib.swissprot.vocabulary.VOID_EXT;

public class ServiceDescriptionStatementGenerator {
	private final RDFHandler handler;
	private long id = 0;
	private final ValueFactory vf;
	public ServiceDescriptionStatementGenerator(RDFHandler handler) {
		this.handler = handler;
		this.vf = SimpleValueFactory.getInstance();
	}

	public void generateStatements(Resource subject, ServiceDescription item){
		item.getTotalTripleCount();
		Resource defaultDatasetId = vf.createBNode(Long.toHexString(id++));
		Resource defaultGraphId = vf.createBNode(Long.toHexString(id++));
		statement(subject, RDF.PROPERTY, SD.SERVICE);
		statement(subject, SD.DEFAULT_DATASET, defaultDatasetId);
		statement(subject, SD.ENDPOINT, subject);
		supportedFormats(subject);
		statement(subject, SD.SUPPORTED_LANGUAGE, SD.SPARQL_11_QUERY);
		statement(subject, SD.PROPERTY_FEATURE, SD.UNION_DEFAULT_GRAPH);
		statement(subject, SD.PROPERTY_FEATURE, SD.BASIC_FEDERATED_QUERY);
		statement(defaultDatasetId, RDF.PROPERTY, SD.DATASET);
		statement(defaultDatasetId, SD.DEFAULT_GRAPH, defaultGraphId);
		if (item.getRelease() != null)
			statement(defaultDatasetId, PAV.VERSION, vf.createLiteral(item.getRelease()));
		statement(defaultGraphId, RDF.PROPERTY, SD.GRAPH_CLASS);
		statement(defaultGraphId, VOID.TRIPLES, vf.createLiteral(item.getTotalTripleCount()));
		
		LocalDate calendar = item.getReleaseDate();
		if (calendar != null) {
			statement(defaultGraphId, DCTERMS.ISSUED, vf.createLiteral(calendar));
			statement(defaultDatasetId, DCTERMS.ISSUED, vf.createLiteral(calendar));
		}

		long distinctObjects = item.getDistinctObjectCount();
		if (distinctObjects > 0)
			statement(defaultGraphId, VOID.DISTINCT_OBJECTS, vf.createLiteral(distinctObjects));

		if (item.getDistinctLiteralObjectCount() > 0)
			statement(defaultGraphId, VOID_EXT.DISTINCT_LITERALS, vf.createLiteral(item.getDistinctLiteralObjectCount()));

		if (item.getDistinctIriObjectCount() > 0)
			statement(defaultGraphId, VOID_EXT.DISTINCT_IRI_REFERENCE_OBJECTS,
					vf.createLiteral(item.getDistinctIriObjectCount()));

		if (item.getDistinctBnodeObjectCount() > 0)
			statement(defaultGraphId, VOID_EXT.DISTINCT_BLANK_NODE_OBJECTS, vf.createLiteral(item.getDistinctBnodeObjectCount()));

		for (GraphDescription gd : item.getGraphs())
			statementsAboutGraph(defaultDatasetId, gd, item);
	}

	protected void statementsAboutGraph(Resource defaultDatasetId, GraphDescription gd, ServiceDescription sd) {
		final String rawGraphName = gd.getGraphName();
		IRI graphName = getIRI(rawGraphName);
		String host = sd.getBase().substring(0, sd.getBase().length() - "sparql/".length());
		IRI namedGraph = graphName;
		statement(defaultDatasetId, SD.NAMED_GRAPH_PROPERTY, namedGraph);
		String voidLocation = host + "/.well-known/void#";
		IRI graph = vf.createIRI(voidLocation, "_graph_" + graphName.getLocalName());
		statement(namedGraph, SD.NAMED_GRAPH_CLASS, graph);
		statement(namedGraph, SD.GRAPH_PROPERTY, graphName);

		statement(graph, RDF.TYPE, SD.GRAPH_CLASS);
		statement(graph, VOID.TRIPLES, vf.createLiteral(gd.getTripleCount()));
		long distinctClasses = gd.getDistinctClassesCount();
		if (distinctClasses > 0)
			statement(graph, VOID.CLASSES, vf.createLiteral(distinctClasses));
		for (ClassPartition cp : gd.getClasses()) {
			final IRI iriOfType = getIRI(cp.getClazz().toString());
			IRI dataSetClassPartition = getResourceForPartition(namedGraph, iriOfType, voidLocation);
			statement(graph, VOID.CLASS_PARTITION, dataSetClassPartition);
			statement(dataSetClassPartition, VOID.CLASS, iriOfType);
			for (PredicatePartition pp : cp.getPredicatePartitions()) {
				IRI ppr = getResourceForSubPartition(namedGraph, cp.getClazz(), pp.getPredicate(), voidLocation);
				statement(dataSetClassPartition, VOID.PROPERTY_PARTITION, ppr);
				statement(ppr, VOID.PROPERTY, pp.getPredicate());
				generateClassPartitions(namedGraph, cp, pp, ppr, voidLocation);
				generateDatatypePartitions(namedGraph, pp, ppr, voidLocation);
				generateSubjectPartitions(namedGraph, pp, ppr, voidLocation);
				if (pp.getTripleCount() > 0L)
					statement(ppr, VOID.TRIPLES, vf.createLiteral(pp.getTripleCount()));
				if (pp.getDistinctSubjectCount() > 0L)
					statement(ppr, VOID.DISTINCT_SUBJECTS, vf.createLiteral(pp.getDistinctSubjectCount()));
				if (pp.getDistinctObjectCount() > 0L)
					statement(ppr, VOID.DISTINCT_OBJECTS, vf.createLiteral(pp.getDistinctObjectCount()));
			}
		}
		for (PredicatePartition predicate : gd.getPredicates()) {
			IRI dataSetPropertyPartition = getResourceForPartition(namedGraph,
					getIRI(predicate.getPredicate()), voidLocation);
			statement(graph, VOID.PROPERTY_PARTITION, dataSetPropertyPartition);
			statement(dataSetPropertyPartition, VOID.PROPERTY, predicate.getPredicate());
			for (ClassPartition ppcp : predicate.getClassPartitions()) {
				Resource cppr = getResourceForSubPartition(namedGraph, predicate.getPredicate(), ppcp.getClazz(),
						voidLocation);
				statement(dataSetPropertyPartition, VOID.CLASS_PARTITION, cppr);
				statement(cppr, VOID.CLASS, ppcp.getClazz());
			}
			generateDatatypePartitions(namedGraph, predicate, dataSetPropertyPartition, voidLocation);
			generateSubjectPartitions(namedGraph, predicate, dataSetPropertyPartition, voidLocation);
			if (predicate.getTripleCount() > 0L)
				statement(dataSetPropertyPartition, VOID.TRIPLES, vf.createLiteral(predicate.getTripleCount()));
			if (predicate.getDistinctSubjectCount() > 0L)
				statement(dataSetPropertyPartition, VOID.DISTINCT_SUBJECTS,vf.createLiteral(
						predicate.getDistinctSubjectCount()));
			if (predicate.getDistinctObjectCount() > 0L)
				statement(dataSetPropertyPartition, VOID.DISTINCT_OBJECTS, vf.createLiteral(predicate.getDistinctObjectCount()));
		}

		long distinctObjects = gd.getDistinctObjectCount();
		if (distinctObjects > 0)
			statement(graph, VOID.DISTINCT_OBJECTS, vf.createLiteral(distinctObjects));

		if (gd.getDistinctLiteralObjectCount() > 0)
			statement(graph, VOID_EXT.DISTINCT_LITERALS, vf.createLiteral(gd.getDistinctLiteralObjectCount()));

		if (gd.getDistinctIriObjectCount() > 0)
			statement(graph, VOID_EXT.DISTINCT_IRI_REFERENCE_OBJECTS, vf.createLiteral(gd.getDistinctIriObjectCount()));

		if (gd.getDistinctBnodeObjectCount() > 0)
			statement(graph, VOID_EXT.DISTINCT_BLANK_NODE_OBJECTS, vf.createLiteral(gd.getDistinctBnodeObjectCount()));

		long distinctSubjects = gd.getDistinctSubjectCount();
		if (distinctSubjects > 0)
			statement(graph, VOID.DISTINCT_SUBJECTS, vf.createLiteral(distinctSubjects));
		if (gd.getDistinctIriSubjectCount() > 0)
			statement(graph, VOID_EXT.DISTINCT_IRI_REFERENCE_SUBJECTS, vf.createLiteral(gd.getDistinctIriSubjectCount()));
		if (gd.getDistinctBnodeSubjectCount() > 0)
			statement(graph, VOID_EXT.DISTINCT_BLANK_NODE_SUBJECTS, vf.createLiteral(gd.getDistinctIriSubjectCount()));
	}

	private void generateClassPartitions(IRI namedGraph, ClassPartition cp, PredicatePartition pp, IRI ppr,
			String voidLocation) {
		for (ClassPartition ppcp : pp.getClassPartitions()) {
			IRI cppr = getResourceForSubPartition(namedGraph, cp.getClazz(), pp.getPredicate(), ppcp.getClazz(),
					voidLocation);
			statement(ppr, VOID.CLASS_PARTITION, cppr);
			statement(cppr, VOID.CLASS, ppcp.getClazz());
		}
	}

	private void generateDatatypePartitions(IRI namedGraph, PredicatePartition pp, IRI ppr,
			String voidLocation) {
		for (DataTypePartition dtpr : pp.getDataTypePartitions()) {
			final IRI datatype = dtpr.getDatatype();
			IRI cppr = getResourceForSubPartition(namedGraph, pp.getPredicate(), datatype, voidLocation);
			statement(ppr, VOID_EXT.DATATYPE_PARTITION, cppr);
			statement(cppr, VOID_EXT.DATATYPE, datatype);
		}
	}

	private void generateSubjectPartitions(IRI namedGraph, PredicatePartition pp, IRI ppr,
			String voidLocation) {
		for (SubjectPartition spr : pp.getSubjectPartitions()) {
			final IRI datatype = spr.getSubject();
			IRI cppr = getResourceForSubPartition(namedGraph, pp.getPredicate(), datatype, voidLocation);
			statement(ppr, VOID_EXT.SUBJECT_PARTITION, cppr);
			statement(cppr, VOID_EXT.SUBJECT, datatype);
		}
	}

	private IRI getResourceForSubPartition(IRI namedGraph, IRI clazz, IRI predicate,
			String voidLocation) {
		IRI partition = getResourceForPartition(namedGraph, clazz, voidLocation);
		IRI subpartition = getResourceForPartition(namedGraph, predicate, voidLocation);
		return vf.createIRI(partition.getNamespace(), partition.getLocalName() + subpartition.getLocalName());
	}

	private IRI getResourceForSubPartition(IRI namedGraph, IRI sourceClass, IRI predicate,
			IRI targetClass, String voidLocation) {
		IRI partition = getResourceForPartition(namedGraph, sourceClass, voidLocation);
		IRI subpartition = getResourceForPartition(namedGraph, predicate, voidLocation);
		IRI subsubpartition = getResourceForPartition(namedGraph, targetClass, voidLocation);
		return vf.createIRI(partition.getNamespace(),
					partition.getLocalName() + subpartition.getLocalName() + subsubpartition.getLocalName());
	}

	protected IRI getResourceForPartition(final IRI namedGraph, final IRI rt, String voidLocation) {
		return vf.createIRI(voidLocation, namedGraph.getLocalName() + '!' + rt.getLocalName());
	}

	protected IRI getIRI(final String rawGraphName) {
		if (rawGraphName.endsWith("/")) {
			final int lastIndexOf = Math.max(rawGraphName.lastIndexOf('/', rawGraphName.length() - 2),
					rawGraphName.lastIndexOf('#'));
			String namespace = rawGraphName.substring(0, lastIndexOf + 1);
			String name = rawGraphName.substring(lastIndexOf + 1, rawGraphName.length() - 1);
			return vf.createIRI(namespace, name);
		} else {
			final int lastIndexOf = Math.max(rawGraphName.lastIndexOf('/', rawGraphName.length() - 1),
					rawGraphName.lastIndexOf('#'));
			String namespace = rawGraphName.substring(0, lastIndexOf + 1);
			String name = rawGraphName.substring(lastIndexOf + 1);
			return vf.createIRI(namespace, name);
		}
	}

	protected IRI getIRI(final Resource graphName) {
		String rawGraphName = graphName.toString();
		if (rawGraphName.endsWith("/")) {
			final int lastIndexOf = Math.max(rawGraphName.lastIndexOf('/', rawGraphName.length() - 2),
					rawGraphName.lastIndexOf('#'));
			String namespace = rawGraphName.substring(0, lastIndexOf + 1);
			String name = rawGraphName.substring(lastIndexOf + 1, rawGraphName.length() - 1);
			return vf.createIRI(namespace, name);
		} else {
			final int lastIndexOf = Math.max(rawGraphName.lastIndexOf('/', rawGraphName.length() - 1),
					rawGraphName.lastIndexOf('#'));
			String namespace = rawGraphName.substring(0, lastIndexOf + 1);
			String name = rawGraphName.substring(lastIndexOf + 1);
			return vf.createIRI(namespace, name);
		}
	}

	protected void supportedFormats(Resource subject) {
		statement(subject, SD.RESULT_FORMAT, FORMATS.CSV);
		statement(subject, SD.RESULT_FORMAT, FORMATS.JSON);
		statement(subject, SD.RESULT_FORMAT, FORMATS.NTRIPLES);
		statement(subject, SD.RESULT_FORMAT, FORMATS.RDF_XML);
		statement(subject, SD.RESULT_FORMAT, FORMATS.TSV);
		statement(subject, SD.RESULT_FORMAT, FORMATS.TURTLE);
		statement(subject, SD.RESULT_FORMAT, FORMATS.XML);
	}

	private void statement(Resource s, IRI p, Value o) {
		handler.handleStatement(vf.createStatement(s, p, o));
		
	}
}
