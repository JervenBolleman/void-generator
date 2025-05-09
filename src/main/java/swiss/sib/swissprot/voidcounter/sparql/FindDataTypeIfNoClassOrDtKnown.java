package swiss.sib.swissprot.voidcounter.sparql;

import java.util.HashSet;
import java.util.Set;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

/**
 * This code is run if we can not find a class earlier or if it was not give.
 * 
 * @author jbollema
 *
 */
final class FindDataTypeIfNoClassOrDtKnown extends QueryCallable<Set<IRI>> {
	private static final Logger log = LoggerFactory.getLogger(FindDataTypeIfNoClassOrDtKnown.class);
	private final String dataTypeQuery;
	private final PredicatePartition predicatePartition;
	private final ClassPartition source;
	private final CommonVariables cv;

	public FindDataTypeIfNoClassOrDtKnown(CommonVariables cv, PredicatePartition predicatePartition, ClassPartition source, OptimizeFor optimizeFor) {
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;
		this.predicatePartition = predicatePartition;
		this.source = source;
		this.dataTypeQuery = Helper.loadSparqlQuery("find_datatypes_if_no_class_or_dt_known", optimizeFor);
	}

	@Override
	protected Set<IRI> run(RepositoryConnection connection) throws Exception {
		return findDatatypeIfNoClassOrDtKnown(source, predicatePartition);
	}

	private Set<IRI> findDatatypeIfNoClassOrDtKnown(ClassPartition source, PredicatePartition predicatePartition)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		final Resource sourceType = source.getClazz();

		Resource predicate = predicatePartition.getPredicate();
		Set<IRI> datatypes = new HashSet<>();
		try (RepositoryConnection connection = repository.getConnection()) {
				pureSparql(predicate, sourceType, connection, datatypes);
		}
		return datatypes;
	}

	private void pureSparql(Resource predicate, Resource sourceType,
			RepositoryConnection localConnection, Set<IRI> datatypes) {


		TupleQuery tq = localConnection.prepareTupleQuery(dataTypeQuery);
		tq.setBinding("graphName", cv.gd().getGraph());
		tq.setBinding("sourceType", sourceType);
		tq.setBinding("predicate", predicate);
		setQuery(dataTypeQuery, tq.getBindings());
		try (TupleQueryResult eval = tq.evaluate()) {
			while (eval.hasNext()) {
				final BindingSet next = eval.next();
				if (next.hasBinding("dt")) {
					final Binding binding = next.getBinding("dt");
					if (binding.getValue() instanceof IRI i)  {
						datatypes.add(i);
					}
				}
			}
		}
	}

	@Override
	protected void logStart() {
		log.debug("Finding distinct datatypes for {} and predicate {}", cv.gd().getGraphName(), predicatePartition.getPredicate());

	}

	@Override
	protected void logEnd() {
		log.debug("Found distinct datatypes for {} and predicate ", cv.gd().getGraphName(), predicatePartition.getPredicate());

	}

	@Override
	protected void set(Set<IRI> t) {

		try {
			cv.writeLock().lock();
			for (IRI datatype : t) {
				predicatePartition.putDatatypeParition(datatype);
			}
		} finally {
			cv.writeLock().unlock();
		}
	}
	
	@Override
	protected Logger getLog() {
		return log;
	}
}
