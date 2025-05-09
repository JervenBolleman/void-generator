package swiss.sib.swissprot.voidcounter.virtuoso;

import java.util.Map;

import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.voidcounter.CommonVariables;

public final class CountDistinctBnodeObjectsInAGraphVirtuoso extends CountDistinctIriInAGraphVirtuoso {

	private static final Logger log = LoggerFactory.getLogger(CountDistinctBnodeObjectsInAGraphVirtuoso.class);

	public CountDistinctBnodeObjectsInAGraphVirtuoso(CommonVariables cv, Map<String, Roaring64NavigableMap> graphIriIds) {
		super(cv, l -> cv.sd().setDistinctIriObjectCount(l), GraphDescription::setDistinctIriObjectCount,
				graphIriIds);

	}

	@Override
	protected void logStart() {
		log.debug("Counting distinct bnode objects for {}", cv.gd().getGraphName());
	}

	@Override
	protected void logFailed(Exception e) {
		if (log.isErrorEnabled())
			log.error("failed counting distinct bnode objects " + cv.gd().getGraphName(), e);
	}

	@Override
	protected void logEnd() {
		log.debug("Counted distinct objects bnode for graph {}", cv.gd().getGraphName());
	}

	@Override
	protected String queryForGraph() {
		return "SELECT iri_id_num(RDF_QUAD.O) from RDF_QUAD table option (index RDF_QUAD_POGS) where isiri_id(RDF_QUAD.O) > 0 AND is_bnode_iri_id(RDF_QUAD.O) > 0 AND RDF_QUAD.G = iri_to_id('"
				+ cv.gd().getGraphName() + "')";
	}

	@Override
	protected Logger getLog() {
		return log;
	}
}