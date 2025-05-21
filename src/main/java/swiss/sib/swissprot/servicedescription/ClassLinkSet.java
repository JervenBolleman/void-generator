package swiss.sib.swissprot.servicedescription;

/**
 * Stores the links between two classes and the predicate used to link them.
 */
public class ClassLinkSet
    implements Comparable<ClassLinkSet>
{
	private String predicate;
	private String target;
	private String source;

	public ClassLinkSet()
	{

	}

	public void setPredicate(String predicate)
	{
		this.predicate = predicate;
	}

	public void setTarget(String target)
	{
		this.target = target;
	}

	public void setSource(String source)
	{
		this.source = source;
	}

	public ClassLinkSet(String source, String predicate, String target)
	{
		super();
		this.source = source;
		this.predicate = predicate;
		this.target = target;
	}

	public String getSource()
	{
		return source;
	}

	public String getPredicate()
	{
		return predicate;
	}

	public String getTarget()
	{
		return target;
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
		ClassLinkSet other = (ClassLinkSet) obj;
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

	@Override
	public int compareTo(ClassLinkSet o)
	{
		int order = String.CASE_INSENSITIVE_ORDER.compare(predicate, o.predicate);
		if (order == 0)
		{
			order = String.CASE_INSENSITIVE_ORDER.compare(source, o.source);
			if (order == 0)
				order = String.CASE_INSENSITIVE_ORDER.compare(target, o.target);
		}
		return order;
	}
}
