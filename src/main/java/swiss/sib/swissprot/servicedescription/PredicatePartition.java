package swiss.sib.swissprot.servicedescription;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.query.algebra.evaluation.util.ValueComparator;


public class PredicatePartition
    implements Comparable<PredicatePartition>
{

	private IRI predicate;
	private final Map<IRI, ClassPartition> classPartitions = Collections.synchronizedMap(new TreeMap<>(new ValueComparator()));
	private final Map<IRI, DataTypePartition> dataTypePartitions = Collections.synchronizedMap(new TreeMap<>(new ValueComparator()));
	private final Map<Resource, SubjectPartition> subjectPartitions = Collections.synchronizedMap(new TreeMap<>(new ValueComparator()));
	private long tripleCount;
	private long subjectCount;
	private long objectCount;

	public PredicatePartition()
	{
		super();
	}

	public PredicatePartition(IRI predicate)
	{
		super();
		this.predicate = predicate;
	}

	public IRI getPredicate()
	{
		return predicate;
	}

	public void setPredicate(IRI predicate)
	{
		this.predicate = predicate;
	}

	public void putClassPartition(ClassPartition classPartition)
	{
		classPartitions.put(classPartition.getClazz(), classPartition);
	}

	public Collection<ClassPartition> getClassPartitions()
	{
		return classPartitions.values();
	}

	public Collection<DataTypePartition> getDataTypePartitions()
	{
		return dataTypePartitions.values();
	}

	@Override
	public int compareTo(PredicatePartition o)
	{
		return predicate.stringValue().compareTo(o.getPredicate().stringValue());
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((classPartitions == null) ? 0 : classPartitions.hashCode());
		result = prime * result + ((dataTypePartitions == null) ? 0 : dataTypePartitions.hashCode());
		result = prime * result + (int) (objectCount ^ (objectCount >>> 32));
		result = prime * result + ((predicate == null) ? 0 : predicate.hashCode());
		result = prime * result + (int) (subjectCount ^ (subjectCount >>> 32));
		result = prime * result + ((subjectPartitions == null) ? 0 : subjectPartitions.hashCode());
		result = prime * result + (int) (tripleCount ^ (tripleCount >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PredicatePartition other = (PredicatePartition) obj;
		if (classPartitions == null)
		{
			if (other.classPartitions != null)
				return false;
		}
		else if (!classPartitions.equals(other.classPartitions))
			return false;
		if (dataTypePartitions == null)
		{
			if (other.dataTypePartitions != null)
				return false;
		}
		else if (!dataTypePartitions.equals(other.dataTypePartitions))
			return false;
		if (objectCount != other.objectCount)
			return false;
		if (predicate == null)
		{
			if (other.predicate != null)
				return false;
		}
		else if (!predicate.equals(other.predicate))
			return false;
		if (subjectCount != other.subjectCount)
			return false;
		if (subjectPartitions == null)
		{
			if (other.subjectPartitions != null)
				return false;
		}
		else if (!subjectPartitions.equals(other.subjectPartitions))
			return false;
		if (tripleCount != other.tripleCount)
			return false;
		return true;
	}

	public void putDatatypeParition(IRI resourceOf)
	{
		dataTypePartitions.put(resourceOf, new DataTypePartition(resourceOf));
	}

	public void putDatatypePartition(DataTypePartition resourceOf)
	{
		dataTypePartitions.put(resourceOf.getDatatype(), resourceOf);
	}

	public void putSubjectPartition(SubjectPartition subTarget)
	{
		subjectPartitions.put(subTarget.getSubject(), subTarget);
	}

	public Collection<SubjectPartition> getSubjectPartitions()
	{
		return subjectPartitions.values();
	}

	public void setTripleCount(long tripleCount) {
		this.tripleCount= tripleCount;
	}

	public long getTripleCount() {
		return tripleCount;
	}

	public void setDistinctSubjectCount(long subjects)
	{
		this.subjectCount = subjects;
	}
	
	public long getDistinctSubjectCount()
	{
		return this.subjectCount;
	}
	
	public void setDistinctObjectCount(long objects)
	{
		this.objectCount = objects;
	}
	
	public long getDistinctObjectCount()
	{
		return this.objectCount;
	}
}
