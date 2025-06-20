package swiss.sib.swissprot.voidcounter.sparql;

import java.util.HashSet;
import java.util.Set;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.voidcounter.CommonGraphVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

/**
 * This code is run if we can not find a class earlier or if it was not give.
 * 
 * @author jbollema
 *
 */
final class FindDataTypeIfNoClassOrDtKnown extends QueryCallable<Set<IRI>, CommonGraphVariables> {
	private static final Logger log = LoggerFactory.getLogger(FindDataTypeIfNoClassOrDtKnown.class);
	private final String dataTypeQuery;
	private final PredicatePartition predicatePartition;
	private final ClassPartition source;

	public FindDataTypeIfNoClassOrDtKnown(CommonGraphVariables cv, PredicatePartition predicatePartition, ClassPartition source, OptimizeFor optimizeFor) {
		super(cv);
		this.predicatePartition = predicatePartition;
		this.source = source;
		this.dataTypeQuery = Helper.loadSparqlQuery("find_datatypes_if_no_class_or_dt_known", optimizeFor);
	}

	@Override
	protected Set<IRI> run(RepositoryConnection connection) throws Exception {
		return findDatatypeIfNoClassOrDtKnown(source, predicatePartition, connection);
	}

	private Set<IRI> findDatatypeIfNoClassOrDtKnown(ClassPartition source, PredicatePartition predicatePartition, RepositoryConnection connection2)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		final Resource sourceType = source.getClazz();

		Resource predicate = predicatePartition.getPredicate();
		Set<IRI> datatypes = new HashSet<>();
		pureSparql(predicate, sourceType, connection2, datatypes);
		return datatypes;
	}

	private void pureSparql(Resource predicate, Resource sourceType,
			RepositoryConnection localConnection, Set<IRI> datatypes) {


		MapBindingSet tq = new MapBindingSet();
		tq.setBinding("graphName", cv.gd().getGraph());
		tq.setBinding("sourceType", sourceType);
		tq.setBinding("predicate", predicate);
		setQuery(dataTypeQuery, tq);
		try (TupleQueryResult eval = Helper.runTupleQuery(getQuery(), localConnection)) {
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
	public Logger getLog() {
		return log;
	}
}
