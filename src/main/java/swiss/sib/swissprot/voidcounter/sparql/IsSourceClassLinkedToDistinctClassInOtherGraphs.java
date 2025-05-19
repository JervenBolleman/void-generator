package swiss.sib.swissprot.voidcounter.sparql;

import static swiss.sib.swissprot.voidcounter.sparql.IsSourceClassLinkedToDistinctClassInOtherGraph.makeQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

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

/**
 * Generates both class paritions in a graph and linkset partitions between graphs
 * using one query.
 */
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

	public IsSourceClassLinkedToDistinctClassInOtherGraphs(CommonVariables cv, PredicatePartition predicatePartition,
			ClassPartition source, String classExclusion, OptimizeFor optimizeFor) {
		super(cv.repository(), cv.limiter(), cv.finishedQueries());
		this.cv = cv;
		this.predicatePartition = predicatePartition;
		this.source = source;
		this.classExclusion = classExclusion;
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

		setQuery(rq, ibs);
		List<LinkSetToOtherGraph> pps = new ArrayList<>();
		try (TupleQueryResult result = Helper.runTupleQuery(getQuery(), connection)) {
			while (result.hasNext()) {
				BindingSet bs = result.next();
				long count = ((Literal) bs.getBinding(SUBJECTS).getValue()).longValue();
				IRI targetType = (IRI) bs.getBinding(TARGET_TYPE).getValue();
				IRI targetGraph = (IRI) bs.getBinding(TARGET_GRAPH).getValue();

				GraphDescription targetGD = cv.sd().getGraph(targetGraph.stringValue());
				if (targetGD != null) {
					LinkSetToOtherGraph subTarget = new LinkSetToOtherGraph(predicatePartition, targetType, sourceType,
							targetGD, cv.gd().getGraph());
					subTarget.setTripleCount(count);
					pps.add(subTarget);
				} else {
					log.debug("Ignoring connection to graph {} by configuration", targetGraph);
				}
			}
		}
		return pps;
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
				if (lstog.getOtherGraph().equals(cv.gd())) {
					ClassPartition subTarget = new ClassPartition(lstog.getTargetType());
					subTarget.setTripleCount(lstog.getTripleCount());
					predicatePartition.putClassPartition(subTarget);
				} else {
					predicatePartition.putLinkPartition(lstog);
				}
			}
		} finally {
			cv.writeLock().unlock();
		}
		cv.save();
	}

	@Override
	protected Logger getLog() {
		return log;
	}

}
