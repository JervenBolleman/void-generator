package swiss.sib.swissprot.servicedescription;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.query.algebra.evaluation.util.ValueComparator;



public abstract class ShapePartition
    implements Comparable<ShapePartition>
{
	private Map<IRI, PredicatePartition> predicateParitions = Collections.synchronizedMap(new TreeMap<>(new ValueComparator()));
	private long count;

	public ShapePartition()
	{
		super();
	}

	public abstract String getShapeMembersSelectionQuery();

	public PredicatePartition getPredicatePartition(Resource predicate)
	{
		return predicateParitions.get(predicate);
	}

	public void putPredicatePartition(PredicatePartition predicatePartition)
	{
		predicateParitions.put(predicatePartition.getPredicate(), predicatePartition);
	}

	@Override
	public int compareTo(ShapePartition o)
	{
		return getShapeMembersSelectionQuery().compareTo(o.getShapeMembersSelectionQuery());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (count ^ (count >>> 32));
		result = prime * result + ((predicateParitions == null) ? 0 : predicateParitions.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		ShapePartition other = (ShapePartition) obj;
		return this.compareTo(other) == 0;
	}

	public Collection<PredicatePartition> getPredicatePartitions()
	{
		return Collections.unmodifiableCollection(predicateParitions.values());
	}

	public void setTripleCount(long valueOf)
	{
		this.count = valueOf;
	}

	public long getTripleCount()
	{
		return count;
	}
}
