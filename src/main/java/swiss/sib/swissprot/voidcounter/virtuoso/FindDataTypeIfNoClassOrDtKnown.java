package swiss.sib.swissprot.voidcounter.virtuoso;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;
import virtuoso.rdf4j.driver.VirtuosoRepositoryConnection;

/**
 * This code is run if we can not find a class earlier or if it was not give.
 * 
 * @author jbollema
 *
 */
final class FindDataTypeIfNoClassOrDtKnown extends QueryCallable<Set<IRI>> {
	private static final Logger log = LoggerFactory.getLogger(FindDataTypeIfNoClassOrDtKnown.class);
	private final PredicatePartition predicatePartition;
	private final ClassPartition source;

	public FindDataTypeIfNoClassOrDtKnown(CommonVariables cv, PredicatePartition predicatePartition,
			ClassPartition source) {
		super(cv);
		this.predicatePartition = predicatePartition;
		this.source = source;
	}

	@Override
	protected Set<IRI> run(RepositoryConnection connection) throws Exception {
		return findDatatypeIfNoClassOrDtKnown(source, predicatePartition, connection);
	}

	private Set<IRI> findDatatypeIfNoClassOrDtKnown(ClassPartition source, PredicatePartition predicatePartition,
			RepositoryConnection connection)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		final Resource sourceType = source.getClazz();

		Resource predicate = predicatePartition.getPredicate();
		Set<IRI> datatypes = new HashSet<>();

		if (connection instanceof VirtuosoRepositoryConnection) {
			virtuosoOptimized(sourceType, predicate, datatypes, connection);
		} else {
			throw new IllegalStateException("Not a virtuoso connection");
		}

		return datatypes;
	}

	private void virtuosoOptimized(final Resource sourceType, Resource predicate, Set<IRI> datatypes,
			RepositoryConnection connection) {
		// See http://docs.openlinksw.com/virtuoso/rdfiriidtype/
		final Connection quadStoreConnection = ((VirtuosoRepositoryConnection) connection).getQuadStoreConnection();
		setQuery("SELECT DISTINCT t.dt FROM (SELECT TOP 100000 RDF_DATATYPE_OF_OBJ(PO.O) dt "
				+ "FROM RDF_QUAD PO, RDF_QUAD ST WHERE  ST.S=PO.S AND "
				+ " ST.P=iri_to_id('http://www.w3.org/1999/02/22-rdf-syntax-ns#type') AND ST.O=iri_to_id('" + sourceType
				+ "') AND " + " PO.P=iri_to_id('" + predicate + "') AND " + " isiri_id(PO.O) = 0 AND "
				+ " PO.G=iri_to_id('" + cv.gd().getGraphName() + "') AND " + " ST.G=iri_to_id('"
				+ cv.gd().getGraphName() + "'))t");
		try (final Statement createStatement = quadStoreConnection.createStatement()) {

			try (ResultSet rs = createStatement.executeQuery(getQuery())) {
				while (rs.next()) {
					IRI datatype = SimpleValueFactory.getInstance().createIRI(rs.getString(1));
					datatypes.add(datatype);
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(getQuery(), e);
		}
	}

	@Override
	protected void logStart() {
		log.debug("Finding distinct datatypes for {} and predicate {}", cv.gd().getGraphName(),
				predicatePartition.getPredicate());

	}

	@Override
	protected void logEnd() {
		log.debug("Found distinct datatypes for {} and predicate ", cv.gd().getGraphName(),
				predicatePartition.getPredicate());

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
