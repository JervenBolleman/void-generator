package swiss.sib.swissprot.voidcounter;

import org.eclipse.rdf4j.model.IRI;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.PredicatePartition;

public class LinkSet {

	private final PredicatePartition predicatePartition;
	private final IRI targetType;
	private final GraphDescription otherGraph;

	public LinkSet(PredicatePartition predicatePartition, IRI targetType, GraphDescription otherGraph) {
		this.predicatePartition = predicatePartition;
		this.targetType = targetType;
		this.otherGraph = otherGraph;
	}

	public PredicatePartition getPredicatePartition() {
		return predicatePartition;
	}

	public IRI getTargetType() {
		return targetType;
	}

	public GraphDescription getOtherGraph() {
		return otherGraph;
	}
}
