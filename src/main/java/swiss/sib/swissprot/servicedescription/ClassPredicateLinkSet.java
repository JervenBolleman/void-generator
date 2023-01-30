package swiss.sib.swissprot.servicedescription;

import org.eclipse.rdf4j.model.Resource;

public class ClassPredicateLinkSet
    implements Comparable<ClassPredicateLinkSet>
{

	private Resource predicate;
	private Resource type;

	public ClassPredicateLinkSet(Resource predicate, Resource type)
	{
		this.predicate = predicate;
		this.type = type;
	}

	public Resource getPredicate()
	{
		return predicate;
	}

	public void setPredicate(Resource predicate)
	{
		this.predicate = predicate;
	}

	public Resource getType()
	{
		return type;
	}

	public void setType(Resource type)
	{
		this.type = type;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((predicate == null) ? 0 : predicate.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
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
		ClassPredicateLinkSet other = (ClassPredicateLinkSet) obj;
		if (predicate == null)
		{
			if (other.predicate != null)
				return false;
		}
		else if (!predicate.equals(other.predicate))
			return false;
		if (type == null)
		{
			if (other.type != null)
				return false;
		}
		else if (!type.equals(other.type))
			return false;
		return true;
	}

	@Override
	public int compareTo(ClassPredicateLinkSet o)
	{
		int order = predicate.stringValue().compareTo(o.predicate.stringValue());
		if (order == 0)
		{
			order = type.stringValue().compareTo(o.type.stringValue());
		}
		return order;
	}
}
