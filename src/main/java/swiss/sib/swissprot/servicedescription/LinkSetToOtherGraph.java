package swiss.sib.swissprot.servicedescription;

import org.eclipse.rdf4j.model.IRI;

public class LinkSetToOtherGraph {

	private final PredicatePartition predicatePartition;
	private final IRI targetType;
	private final GraphDescription otherGraph;
	private long count;
	private final IRI sourceType;

	public LinkSetToOtherGraph(PredicatePartition predicatePartition, IRI targetType, IRI sourceType, GraphDescription otherGraph) {
		this.predicatePartition = predicatePartition;
		this.targetType = targetType;
		this.sourceType = sourceType;
		this.otherGraph = otherGraph;
	}

	public PredicatePartition getPredicatePartition() {
		return predicatePartition;
	}

	public IRI getTargetType() {
		return targetType;
	}
	
	public IRI getSourceType() {
		return sourceType;
	}

	public GraphDescription getOtherGraph() {
		return otherGraph;
	}

	public void setTripleCount(long count) {
		this.count = count;
	}

	public long getTripleCount() {
		return count;
	}
}
