package swiss.sib.swissprot.voidcounter.sparql;

import static swiss.sib.swissprot.voidcounter.sparql.IsSourceClassLinkedToDistinctClassInOtherGraph.insertAfterWhere;
import static swiss.sib.swissprot.voidcounter.sparql.IsSourceClassLinkedToDistinctClassInOtherGraph.insertBeforeEnd;
import static swiss.sib.swissprot.voidcounter.sparql.IsSourceClassLinkedToDistinctClassInOtherGraph.makeQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.LinkSetToOtherGraph;
import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.servicedescription.sparql.Helper;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.QueryCallable;

public class IsSourceClassLinkedToDistinctClassInOtherGraphs extends QueryCallable<List<LinkSetToOtherGraph>> {
	private static final Logger log = LoggerFactory.getLogger(IsSourceClassLinkedToDistinctClassInOtherGraphs.class);
	private static final String SUBJECTS = "count";
	private static final String TARGET_TYPE = "clazz";
	private static final String TARGET_GRAPH = "targetGraph";
	private final CommonVariables cv;
	private final PredicatePartition predicatePartition;
	private final ClassPartition source;
	private final String classExclusion;
	private final String rawQuery;
	private final OptimizeFor optimizeFor;

	public IsSourceClassLinkedToDistinctClassInOtherGraphs(CommonVariables cv, PredicatePartition predicatePartition,
			ClassPartition source, String classExclusion, OptimizeFor optimizeFor) {
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;
		this.predicatePartition = predicatePartition;
		this.source = source;
		this.classExclusion = classExclusion;
		this.optimizeFor = optimizeFor;
		this.rawQuery = Helper.loadSparqlQuery("inter_graph_links_grouped_by_type_and_target_graph", optimizeFor);
	}

	@Override
	protected void logStart() {
		if (log.isDebugEnabled())
			log.debug("Checking if {} connected to any via {} in {}", source.getClazz(),
					predicatePartition.getPredicate(), cv.gd().getGraphName());
	}

	@Override
	protected void logEnd() {
		if (log.isDebugEnabled())
			log.debug("Checked if {} connected to any via {} in {}", source.getClazz(),
					predicatePartition.getPredicate(), cv.gd().getGraphName());

	}

	@Override
	protected List<LinkSetToOtherGraph> run(RepositoryConnection connection) throws Exception {
		final IRI sourceType = source.getClazz();
		MapBindingSet ibs = new MapBindingSet();
		ibs.setBinding("sourceType", sourceType);
		ibs.setBinding("sourceGraph", cv.gd().getGraph());
		ibs.setBinding("predicate", predicatePartition.getPredicate());
		String rq = makeQuery(rawQuery, knownClasses(), classExclusion);
		String query ;
		if (optimizeFor == OptimizeFor.QLEVER) {
			query = insertBeforeEnd(rq, filterNotGraph());
		} else {
			query = insertAfterWhere(rq, targetGraphValuesClause());
		}

		setQuery(query, ibs);
		List<LinkSetToOtherGraph> pps = new ArrayList<>();
		try (TupleQueryResult result = Helper.runTupleQuery(getQuery(), connection)) {
			while (result.hasNext()) {
				BindingSet bs = result.next();
				long count = ((Literal) bs.getBinding(SUBJECTS).getValue()).longValue();
				IRI targetType = (IRI) bs.getBinding(TARGET_TYPE).getValue();
				IRI targetGraph = (IRI) bs.getBinding(TARGET_GRAPH).getValue();

				LinkSetToOtherGraph subTarget = new LinkSetToOtherGraph(predicatePartition, targetType, sourceType,
						cv.sd().getGraph(targetGraph.stringValue()), cv.gd().getGraph());
				subTarget.setTripleCount(count);
				pps.add(subTarget);
			}
		}
		return pps;
	}

	private String targetGraphValuesClause() {
		IRI sourceGraph = cv.gd().getGraph();
		return cv.sd().getGraphs().stream().map(GraphDescription::getGraph).filter(g -> !g.equals(sourceGraph))
				.map(IRI::stringValue).map(s -> "(<" + s + ">)")
				.collect(Collectors.joining(" ", "\nVALUES ( ?targetGraph ) {", "}"));
	}
	
	private String filterNotGraph() {
		IRI sourceGraph = cv.gd().getGraph();
		return cv.sd().getGraphs().stream().map(GraphDescription::getGraph).filter(g -> !g.equals(sourceGraph))
				.map(IRI::stringValue).map(s -> "(<" + s + ">)")
				.collect(Collectors.joining(", ", "\nFILTER (?targetGraph IN  (", "))"));
	}

	private Set<ClassPartition> knownClasses() {
		// TODO need to figure out if we have the known classes in all the other graphs
		// already
		return Collections.emptySet();
	}

	@Override
	protected void set(List<LinkSetToOtherGraph> t) {
		try {
			cv.writeLock().lock();
			for (LinkSetToOtherGraph lstog : t) {
				predicatePartition.putLinkPartition(lstog);
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
