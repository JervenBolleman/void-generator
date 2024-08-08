package swiss.sib.swissprot.servicedescription;

import java.time.LocalDate;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.DCTERMS;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.SD;
import org.eclipse.rdf4j.model.vocabulary.VOID;
import org.eclipse.rdf4j.query.algebra.evaluation.function.hash.MD5;
import org.eclipse.rdf4j.rio.RDFHandler;

import swiss.sib.swissprot.vocabulary.FORMATS;
import swiss.sib.swissprot.vocabulary.PAV;
import swiss.sib.swissprot.vocabulary.VOID_EXT;
import swiss.sib.swissprot.voidcounter.LinkSet;

public class ServiceDescriptionStatementGenerator {
	private final RDFHandler handler;
	private long id = 0;
	private final ValueFactory vf;

	public ServiceDescriptionStatementGenerator(RDFHandler handler) {
		this.handler = handler;
		this.vf = SimpleValueFactory.getInstance();
	}

	public void generateStatements(IRI iriOfVoid, ServiceDescription item) {

		Resource defaultDatasetId = vf.createBNode(Long.toHexString(id++));
		Resource defaultGraphId = vf.createBNode(Long.toHexString(id++));
		statement(iriOfVoid, RDF.TYPE, SD.SERVICE);
		statement(iriOfVoid, SD.DEFAULT_DATASET, defaultDatasetId);
		statement(iriOfVoid, SD.ENDPOINT, iriOfVoid);
		supportedFormats(iriOfVoid);
		statement(iriOfVoid, SD.SUPPORTED_LANGUAGE, SD.SPARQL_11_QUERY);
		statement(iriOfVoid, SD.PROPERTY_FEATURE, SD.UNION_DEFAULT_GRAPH);
		statement(iriOfVoid, SD.PROPERTY_FEATURE, SD.BASIC_FEDERATED_QUERY);
		statement(defaultDatasetId, RDF.TYPE, SD.DATASET);
		statement(defaultDatasetId, SD.DEFAULT_GRAPH, defaultGraphId);
		if (item.getRelease() != null)
			statement(defaultDatasetId, PAV.VERSION, vf.createLiteral(item.getRelease()));
		statement(defaultGraphId, RDF.TYPE, SD.GRAPH_CLASS);
		if (item.getTotalTripleCount() > 0) {
			statement(defaultGraphId, VOID.TRIPLES, vf.createLiteral(item.getTotalTripleCount()));
		}

		LocalDate calendar = item.getReleaseDate();
		if (calendar != null) {
			statement(defaultGraphId, DCTERMS.ISSUED, vf.createLiteral(calendar));
			statement(defaultDatasetId, DCTERMS.ISSUED, vf.createLiteral(calendar));
		}

		long distinctObjects = item.getDistinctObjectCount();
		if (distinctObjects > 0)
			statement(defaultGraphId, VOID.DISTINCT_OBJECTS, vf.createLiteral(distinctObjects));

		if (item.getDistinctLiteralObjectCount() > 0)
			statement(defaultGraphId, VOID_EXT.DISTINCT_LITERALS,
					vf.createLiteral(item.getDistinctLiteralObjectCount()));

		if (item.getDistinctIriObjectCount() > 0)
			statement(defaultGraphId, VOID_EXT.DISTINCT_IRI_REFERENCE_OBJECTS,
					vf.createLiteral(item.getDistinctIriObjectCount()));

		if (item.getDistinctBnodeObjectCount() > 0)
			statement(defaultGraphId, VOID_EXT.DISTINCT_BLANK_NODE_OBJECTS,
					vf.createLiteral(item.getDistinctBnodeObjectCount()));

		long distinctSubjects = item.getDistinctSubjectCount();
		if (distinctSubjects > 0)
			statement(defaultGraphId, VOID.DISTINCT_SUBJECTS, vf.createLiteral(distinctSubjects));
		if (item.getDistinctIriSubjectCount() > 0)
			statement(defaultGraphId, VOID_EXT.DISTINCT_IRI_REFERENCE_SUBJECTS,
					vf.createLiteral(item.getDistinctIriSubjectCount()));
		if (item.getDistinctBnodeSubjectCount() > 0)
			statement(defaultGraphId, VOID_EXT.DISTINCT_BLANK_NODE_SUBJECTS,
					vf.createLiteral(item.getDistinctBnodeSubjectCount()));

		for (GraphDescription gd : item.getGraphs())
			statementsAboutGraph(defaultDatasetId, gd, item, iriOfVoid);
	}

	protected void statementsAboutGraph(Resource defaultDatasetId, GraphDescription gd, ServiceDescription sd,
			IRI iriOfVoid) {
		final String rawGraphName = gd.getGraphName();
		IRI graphName = getIRI(rawGraphName);
		String voidLocation = iriOfVoid.stringValue();

		IRI namedGraph = graphName;
		statement(defaultDatasetId, SD.NAMED_GRAPH_PROPERTY, namedGraph);
		IRI graph = vf.createIRI(voidLocation, "#_graph_" + graphName.getLocalName());
		statement(namedGraph, SD.NAME, graphName);
		statement(namedGraph, SD.GRAPH_PROPERTY, graph);

		statement(graph, RDF.TYPE, SD.GRAPH_CLASS);
		statement(graph, VOID.ENTITIES, vf.createLiteral(gd.getTripleCount()));
		long distinctClasses = gd.getDistinctClassesCount();
		if (distinctClasses > 0)
			statement(graph, VOID.CLASSES, vf.createLiteral(distinctClasses));
		for (ClassPartition cp : gd.getClasses()) {
			generateClassPartitions(voidLocation, namedGraph, graph, cp);
		}

		for (PredicatePartition predicate : gd.getPredicates()) {
			generatePredicatePartions(voidLocation, namedGraph, graph, predicate);
		}

		if (gd.getShapePartions() != null) {
			for (ShapePartition shape : gd.getShapePartions()) {
				generateShapePartions(voidLocation, namedGraph, graph, shape);
			}
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
			statement(graph, VOID_EXT.DISTINCT_IRI_REFERENCE_SUBJECTS,
					vf.createLiteral(gd.getDistinctIriSubjectCount()));
		if (gd.getDistinctBnodeSubjectCount() > 0)
			statement(graph, VOID_EXT.DISTINCT_BLANK_NODE_SUBJECTS,
					vf.createLiteral(gd.getDistinctBnodeSubjectCount()));
	}

	private void generateShapePartions(String voidLocation, IRI namedGraph, IRI graph, ShapePartition shape) {
		Resource dataSetPropertyPartition = vf.createBNode();  
		statement(graph, VOID.PROPERTY_PARTITION, dataSetPropertyPartition);
		if (shape instanceof ShapeForSubclassOfPartition) {
			statement(graph, RDFS.COMMENT,
					vf.createLiteral("TODO count stuff:" + shape.getShapeMembersSelectionQuery()));	
		}
	}

	private void generateClassPartitions(String voidLocation, IRI namedGraph, IRI graph, ClassPartition cp) {
		final IRI iriOfType = getIRI(cp.getClazz().toString());
		IRI dataSetClassPartition = getResourceForPartition(namedGraph, iriOfType, voidLocation);
		statement(graph, VOID.CLASS_PARTITION, dataSetClassPartition);
		statement(dataSetClassPartition, RDF.TYPE, VOID.DATASET);
		statement(dataSetClassPartition, VOID.CLASS, iriOfType);
		if (cp.getTripleCount() > 0) {
			statement(dataSetClassPartition, VOID.ENTITIES, vf.createLiteral(cp.getTripleCount()));
		}
		for (PredicatePartition pp : cp.getPredicatePartitions()) {
			IRI ppr = getResourceForSubPartition(namedGraph, cp.getClazz(), pp.getPredicate(), voidLocation);
			statement(dataSetClassPartition, VOID.PROPERTY_PARTITION, ppr);
			statement(ppr, RDF.TYPE, VOID.DATASET);
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

	private void generatePredicatePartions(String voidLocation, IRI namedGraph, IRI graph,
			PredicatePartition predicate) {
		IRI dataSetPropertyPartition = getResourceForPartition(namedGraph, getIRI(predicate.getPredicate()),
				voidLocation);
		statement(graph, VOID.PROPERTY_PARTITION, dataSetPropertyPartition);
		statement(dataSetPropertyPartition, VOID.PROPERTY, predicate.getPredicate());
		for (ClassPartition ppcp : predicate.getClassPartitions()) {
			Resource cppr = getResourceForSubPartition(namedGraph, predicate.getPredicate(), ppcp.getClazz(),
					voidLocation);
			statement(dataSetPropertyPartition, VOID.CLASS_PARTITION, cppr);
			statement(cppr, VOID.CLASS, ppcp.getClazz());
			if (ppcp.getTripleCount() > 0) {
				statement(cppr, VOID.ENTITIES, vf.createLiteral(ppcp.getTripleCount()));
			}
		}
		for (LinkSet ls: predicate.getLinkSets()) {
			//TODO generate nicer IRI
			Resource bNode = vf.createBNode();
			statement(bNode, RDF.TYPE, VOID.LINKSET);
			statement(bNode, VOID.LINK_PREDICATE, predicate.getPredicate());
			statement(bNode, VOID.TARGET, dataSetPropertyPartition);
			for (ClassPartition cp:ls.getOtherGraph().getClasses()) {
				if (cp.getClazz().equals(ls.getTargetType()))
					statement(bNode, VOID.TARGET, ls.getTargetType());
			}
		}
		generateDatatypePartitions(namedGraph, predicate, dataSetPropertyPartition, voidLocation);
		generateSubjectPartitions(namedGraph, predicate, dataSetPropertyPartition, voidLocation);
		if (predicate.getTripleCount() > 0L)
			statement(dataSetPropertyPartition, VOID.TRIPLES, vf.createLiteral(predicate.getTripleCount()));
		if (predicate.getDistinctSubjectCount() > 0L)
			statement(dataSetPropertyPartition, VOID.DISTINCT_SUBJECTS,
					vf.createLiteral(predicate.getDistinctSubjectCount()));
		if (predicate.getDistinctObjectCount() > 0L)
			statement(dataSetPropertyPartition, VOID.DISTINCT_OBJECTS,
					vf.createLiteral(predicate.getDistinctObjectCount()));
	}

	private void generateClassPartitions(IRI namedGraph, ClassPartition cp, PredicatePartition pp, IRI ppr,
			String voidLocation) {
		for (ClassPartition ppcp : pp.getClassPartitions()) {
			IRI cppr = getResourceForSubPartition(namedGraph, cp.getClazz(), pp.getPredicate(), ppcp.getClazz(),
					voidLocation);
			statement(ppr, VOID.CLASS_PARTITION, cppr);
			statement(cppr, RDF.TYPE, VOID.DATASET);
			statement(cppr, VOID.CLASS, ppcp.getClazz());
			if (ppcp.getTripleCount() > 0) {
				statement(cppr, VOID.TRIPLES, vf.createLiteral(ppcp.getTripleCount()));
			}
		}
	}

	private void generateDatatypePartitions(IRI namedGraph, PredicatePartition pp, IRI ppr, String voidLocation) {
		for (DataTypePartition dtpr : pp.getDataTypePartitions()) {
			final IRI datatype = dtpr.getDatatype();
			IRI cppr = getResourceForSubPartition(namedGraph, pp.getPredicate(), datatype, voidLocation);
			statement(ppr, VOID_EXT.DATATYPE_PARTITION, cppr);
			statement(cppr, RDF.TYPE, VOID.DATASET);
			statement(cppr, VOID_EXT.DATATYPE, datatype);
			if (dtpr.getTripleCount() > 0) {
				statement(cppr, VOID.TRIPLES, vf.createLiteral(dtpr.getTripleCount()));
			}
		}
	}

	private void generateSubjectPartitions(IRI namedGraph, PredicatePartition pp, IRI ppr, String voidLocation) {
		for (SubjectPartition spr : pp.getSubjectPartitions()) {
			final IRI datatype = spr.getSubject();
			IRI cppr = getResourceForSubPartition(namedGraph, pp.getPredicate(), datatype, voidLocation);
			statement(ppr, VOID_EXT.SUBJECT_PARTITION, cppr);
			statement(cppr, VOID_EXT.SUBJECT, datatype);
			if (spr.getTripleCount() > 0) {
				statement(cppr, VOID.TRIPLES, vf.createLiteral(spr.getTripleCount()));
			}
		}
	}

	private IRI getResourceForSubPartition(IRI namedGraph, IRI clazz, IRI predicate, String voidLocation) {
		IRI partition = getResourceForPartition(namedGraph, clazz, voidLocation);
		IRI subpartition = getResourceForPartition(namedGraph, predicate, voidLocation);
		return vf.createIRI(partition.getNamespace(), partition.getLocalName() + subpartition.getLocalName());
	}

	private IRI getResourceForSubPartition(IRI namedGraph, IRI sourceClass, IRI predicate, IRI targetClass,
			String voidLocation) {
		IRI partition = getResourceForPartition(namedGraph, sourceClass, voidLocation);
		IRI subpartition = getResourceForPartition(namedGraph, predicate, voidLocation);
		IRI subsubpartition = getResourceForPartition(namedGraph, targetClass, voidLocation);
		return vf.createIRI(partition.getNamespace(),
				partition.getLocalName() + subpartition.getLocalName() + subsubpartition.getLocalName());
	}

	protected IRI getResourceForPartition(final IRI namedGraph, final IRI rt, String voidLocation) {
		String md5 = new MD5().evaluate(vf, vf.createLiteral(rt.stringValue())).stringValue();
		if (voidLocation.endsWith("#")) {
			return vf.createIRI(voidLocation, namedGraph.getLocalName() + '!' + md5 + '!' + rt.getLocalName());
		} else {
			return vf.createIRI(voidLocation + "#", namedGraph.getLocalName() + '!' + md5 + '!' + rt.getLocalName());
		}
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
