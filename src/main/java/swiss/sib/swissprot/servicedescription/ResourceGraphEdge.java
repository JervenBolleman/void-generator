package swiss.sib.swissprot.servicedescription;

import org.eclipse.rdf4j.model.Resource;


public class ResourceGraphEdge
{
	private final Resource source;
	private final Resource target;
	private Resource predicate;

	public ResourceGraphEdge(Resource source, Resource predicate, Resource target)
	{
		this.source = source;
		this.predicate = predicate;
		this.target = target;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((predicate == null) ? 0 : predicate.hashCode());
		result = prime * result + ((source == null) ? 0 : source.hashCode());
		result = prime * result + ((target == null) ? 0 : target.hashCode());
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
		ResourceGraphEdge other = (ResourceGraphEdge) obj;
		if (predicate == null)
		{
			if (other.predicate != null)
				return false;
		}
		else if (!predicate.equals(other.predicate))
			return false;
		if (source == null)
		{
			if (other.source != null)
				return false;
		}
		else if (!source.equals(other.source))
			return false;
		if (target == null)
		{
			if (other.target != null)
				return false;
		}
		else if (!target.equals(other.target))
			return false;
		return true;
	}


}
