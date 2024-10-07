package swiss.sib.swissprot.vocabulary;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.base.InternedIRI;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;

public class VOID_EXT {
	public static final String NAMESPACE = "http://ldf.fi/void-ext#";
	public static final String PREFIX = "void_ext";

	public static final IRI DATATYPE = new InternedIRI(NAMESPACE, "datatype");
	public static final IRI IRI_LENGTH_PARTITION = new InternedIRI(NAMESPACE, "iriLengthPartition");
	public static final IRI LITERAL_LENGTH_PARTITION = new InternedIRI(NAMESPACE, "literalLengthPartition");
	public static final IRI NAMESPACE_PROPERTY = new InternedIRI(NAMESPACE, "namespace");
	public static final IRI OBJECT = new InternedIRI(NAMESPACE, "object");
	public static final IRI OBJECT_CLASS_PARTITION = new InternedIRI(NAMESPACE, "objectClassPartition");
	public static final IRI DATATYPE_PARTITION = new InternedIRI(NAMESPACE, "datatypePartition");
	public static final IRI OBJECT_IRI_LENGTH_PARTITION = new InternedIRI(NAMESPACE,
			"objectIRILengthPartition");
	public static final IRI LANGUAGE_PARTITION = new InternedIRI(NAMESPACE, "languagePartition");
	public static final IRI OBJECT_NAMESPACE_PARTITION = new InternedIRI(NAMESPACE,
			"objectNamespacePartition");
	public static final IRI OBJECT_PARTITION = new InternedIRI(NAMESPACE, "objectPartition");
	public static final IRI CLASS_PARTITION = new InternedIRI(NAMESPACE, "propertyClassPartition");
	public static final IRI PROPERTY_IRI_LENGTH_PARTITION = new InternedIRI(NAMESPACE,
			"propertyIRILengthPartition");
	public static final IRI PROPERTY_NAMESPACE_PARTITION = new InternedIRI(NAMESPACE,
			"propertyNamespacePartition");
	public static final IRI SUBJECT = new InternedIRI(NAMESPACE, "subject");
	public static final IRI SUBJECT_IRI_LENGTH_PARTITION = new InternedIRI(NAMESPACE,
			"subjectIRILengthPartition");
	public static final IRI SUBJECT_NAMESPACE_PARTITION = new InternedIRI(NAMESPACE,
			"subjectNamespacePartition");
	public static final IRI SUBJECT_PARTITION = new InternedIRI(NAMESPACE, "subjectPartition");

	public static final IRI AVERAGE_IRI_LENGTH = new InternedIRI(NAMESPACE, "averageIRILength");
	public static final IRI AVERAGE_LITERAL_LENGTH = new InternedIRI(NAMESPACE, "averageLiteralLength");
	public static final IRI AVERAGE_OBJECT_IRI_LENGTH = new InternedIRI(NAMESPACE, "averageObjectIRILength");
	public static final IRI AVERAGE_PROPERTY_IRI_LENGTH = new InternedIRI(NAMESPACE,
			"averagePropertyIRILength");
	public static final IRI AVERAGE_SUBJECT_IRI_LENGTH = new InternedIRI(NAMESPACE, "averageSubjectIRILength");
	public static final IRI DATATYPES = new InternedIRI(NAMESPACE, "datatypes");
	public static final IRI DISTINCT_BLANK_NODE_OBJECTS = new InternedIRI(NAMESPACE,
			"distinctBlankNodeObjects");
	public static final IRI DISTINCT_BLANK_NODE_SUBJECTS = new InternedIRI(NAMESPACE,
			"distinctBlankNodeSubjects");
	public static final IRI DISTINCT_BLANK_NODES = new InternedIRI(NAMESPACE, "distinctBlankNodes");
	public static final IRI DISTINCT_IRI_REFERENCE_OBJECTS = new InternedIRI(NAMESPACE,
			"distinctIRIReferenceObjects");
	public static final IRI DISTINCT_IRI_REFERENCE_SUBJECTS = new InternedIRI(NAMESPACE,
			"distinctIRIReferenceSubjects");
	public static final IRI DISTINCT_IRI_REFERENCES = new InternedIRI(NAMESPACE, "distinctIRIReferences");
	public static final IRI DISTINCT_LITERALS = new InternedIRI(NAMESPACE, "distinctLiterals");
	public static final IRI DISTINCT_RDF_NODES = new InternedIRI(NAMESPACE, "distinctRDFNodes");
	public static final IRI LANGUAGE = new InternedIRI(NAMESPACE, "language");
	public static final IRI LANGUAGES = new InternedIRI(NAMESPACE, "languages");
	public static final IRI LENGTH = new InternedIRI(NAMESPACE, "length");
	public static final IRI MIN_LENGTH = new InternedIRI(NAMESPACE, "minLength");
	public static final IRI OBJECT_CLASSES = new InternedIRI(NAMESPACE, "objectClasses");
	public static final IRI PROPERTY_CLASSES = new InternedIRI(NAMESPACE, "propertyClasses");
	public static final IRI SUBJECT_CLASSES = new InternedIRI(NAMESPACE, "subjectClasses");
	public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);

}
