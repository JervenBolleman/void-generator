package swiss.sib.swissprot.voidcounter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.virtuoso.VirtuosoFromSQL;
import virtuoso.rdf4j.driver.VirtuosoRepositoryConnection;


/**
 * This code is run if we can not find a class earlier or if it was not give.
 * 
 * @author jbollema
 *
 */
public final class FindDataTypeIfNoClassOrDtKnown extends QueryCallable<Set<IRI>> {

	private final PredicatePartition predicatePartition;
	private final ClassPartition source;
	private final GraphDescription gd;
	private final Lock writeLock;
	public static final Logger log = LoggerFactory.getLogger(FindDataTypeIfNoClassOrDtKnown.class);

	public FindDataTypeIfNoClassOrDtKnown(PredicatePartition predicatePartition, ClassPartition source,
			Repository repository, GraphDescription gd, Lock writeLock, Semaphore limiter) {
		super(repository, limiter);
		this.predicatePartition = predicatePartition;
		this.source = source;
		this.gd = gd;
		this.writeLock = writeLock;
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
			if (connection instanceof VirtuosoRepositoryConnection) {
				virtuosoOptimized(sourceType, predicate, datatypes, connection);
			} else {
				pureSparql(predicate, predicatePartition, sourceType, connection, datatypes);
			}
		}
		return datatypes;
	}

	private void virtuosoOptimized(final Resource sourceType, Resource predicate, Set<IRI> datatypes,
			RepositoryConnection connection) {
		// See http://docs.openlinksw.com/virtuoso/rdfiriidtype/
		final Connection quadStoreConnection = ((VirtuosoRepositoryConnection) connection).getQuadStoreConnection();
		String datatypeQuery = "SELECT DISTINCT t.dt FROM (SELECT TOP 100000 RDF_DATATYPE_OF_OBJ(PO.O) dt "
				+ "FROM RDF_QUAD PO, RDF_QUAD ST WHERE  ST.S=PO.S AND "
				+ " ST.P=iri_to_id('http://www.w3.org/1999/02/22-rdf-syntax-ns#type') AND ST.O=iri_to_id('" + sourceType
				+ "') AND " + " PO.P=iri_to_id('" + predicate + "') AND " + " isiri_id(PO.O) = 0 AND "
				+ " PO.G=iri_to_id('" + gd.getGraphName() + "') AND " + " ST.G=iri_to_id('" + gd.getGraphName()
				+ "'))t";
		try (final Statement createStatement = quadStoreConnection.createStatement()) {

			try (ResultSet rs = createStatement.executeQuery(datatypeQuery)) {
				while (rs.next()) {
					IRI datatype = SimpleValueFactory.getInstance().createIRI(rs.getString(1));
					datatypes.add(datatype);
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(datatypeQuery, e);
		}
	}

	private void pureSparql(Resource predicate, PredicatePartition predicatePartition, final Resource sourceType,
			RepositoryConnection localConnection, Set<IRI> datatypes) {
		final String dataTypeQuery = "SELECT ?dt {GRAPH <" + gd.getGraphName() + "> { ?subject a <" + sourceType
				+ "> . ?subject <" + predicate + "> ?target . FILTER(isLiteral(?target)) . BIND(datatype(?target) as ?dt) }} LIMIT 1";

		try (TupleQueryResult eval = VirtuosoFromSQL.runTupleQuery(dataTypeQuery, localConnection)) {
			while (eval.hasNext()) {
				final BindingSet next = eval.next();
				if (next.hasBinding("dt")) {
					final Binding binding = next.getBinding("dt");
					if (binding.getValue() != null ) {
						datatypes.add((IRI) binding.getValue());
					}
				}
			}
		}
	}

	@Override
	protected void logStart() {
		log.debug("Finding distinct datatypes for " + gd.getGraphName() + " and predicate "
				+ predicatePartition.getPredicate());

	}

	@Override
	protected void logEnd() {
		log.debug("Found distinct datatypes for " + gd.getGraphName() + " and predicate "
				+ predicatePartition.getPredicate());

	}

	@Override
	protected void set(Set<IRI> t) {

		try {
			writeLock.lock();
			for (IRI datatype : t) {
				predicatePartition.putDatatypeParition(datatype);
			}
		} finally {
			writeLock.unlock();
		}
	}
}