package swiss.sib.swissprot.voidcounter.virtuoso;

import java.util.Map;

import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.voidcounter.CommonVariables;

public final class CountDistinctIriSubjectsInAGraphVirtuoso extends CountDistinctIriInAGraphVirtuoso {

	private static final Logger log = LoggerFactory.getLogger(CountDistinctIriSubjectsInAGraphVirtuoso.class);

	

	public CountDistinctIriSubjectsInAGraphVirtuoso(CommonVariables cv, Map<String, Roaring64NavigableMap> graphIriIds) {
		super(cv, (l) -> cv.sd().setDistinctIriSubjectCount(l), GraphDescription::setDistinctIriSubjectCount, graphIriIds);
	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct iri subjects for {}", cv.gd().getGraphName());
	}

	@Override
	protected void logFailed(Exception e) {
		if (log.isErrorEnabled())
			log.error("failed counting distinct iri subjects " + cv.gd().getGraphName(), e);
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct iri subjects for graph {}", cv.gd().getGraphName());
	}

	@Override
	protected String queryForGraph() {
		String graphIri = cv.gd().getGraphName();
		return "select iri_id_num(RDF_QUAD.S) from RDF_QUAD table option (index RDF_QUAD_GS) where RDF_QUAD.G = iri_to_id('" + graphIri+ "') and isiri_id(RDF_QUAD.S) > 0 and is_bnode_iri_id(RDF_QUAD.S) = 0";
	}
	
	@Override
	protected Logger getLog() {
		return log;
	}
}