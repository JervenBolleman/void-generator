package swiss.sib.swissprot.servicedescription;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.query.algebra.evaluation.util.ValueComparator;



public class ClassPartition
    implements Comparable<ClassPartition>
{
	private IRI clazz;
	private Map<IRI, PredicatePartition> predicateParitions = Collections.synchronizedMap(new TreeMap<>(new ValueComparator()));
	private long count;

	public void setClazz(IRI clazz)
	{
		this.clazz = clazz;
	}

	public ClassPartition(IRI clazz)
	{
		super();
		this.clazz = clazz;
	}

	public ClassPartition()
	{
		super();
	}

	public IRI getClazz()
	{
		return clazz;
	}

	public PredicatePartition getPredicatePartition(Resource predicate)
	{
		return predicateParitions.get(predicate);
	}

	public void putPredicatePartition(PredicatePartition predicatePartition)
	{
		predicateParitions.put(predicatePartition.getPredicate(), predicatePartition);
	}

	@Override
	public int compareTo(ClassPartition o)
	{
		return clazz.stringValue().compareTo(o.getClazz().stringValue());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((clazz == null) ? 0 : clazz.hashCode());
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
		if (getClass() != obj.getClass())
			return false;
		ClassPartition other = (ClassPartition) obj;
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
