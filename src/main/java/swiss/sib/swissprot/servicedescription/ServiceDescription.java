package swiss.sib.swissprot.servicedescription;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


public class ServiceDescription
{
	private final Map<String, GraphDescription> gds = Collections.synchronizedMap(new TreeMap<>());
	private long totalTripleCount;
	private String release;
	private LocalDate releaseDate;
	
	private String title;
	private long distinctBnodeObjectCount;
	private long distinctIriObjectCount;
	private long distinctLiteralObjectCount;
	private long distinctBnodeSubjectCount;
	private long distinctIriSubjectCount;

	public ServiceDescription()
	{

	}
	
	public void reset() {
		gds.clear();
		totalTripleCount = 0;
		releaseDate = null;
		title = null;
		distinctBnodeObjectCount = 0L;
		distinctIriObjectCount = 0L;
		distinctLiteralObjectCount = 0L;
		distinctBnodeSubjectCount = 0L;
		distinctIriSubjectCount = 0L;
	}

	public GraphDescription getGraph(String graphName)
	{
		return gds.get(graphName);
	}

	public Collection<GraphDescription> getGraphs()
	{
		return sortByGraphSize(gds.values());
	}

	/**
	 * Makes hashes a bit more consistent and in the long run makes the main page a bit nicer to read.
	 * @param graphs the list to be sorted (not modified you get a sorted copy back).
	 * @return sorted graph description objects sorted by name.
	 */
	protected List<GraphDescription> sortByGraphSize(Collection<GraphDescription> graphs)
	{
		List<GraphDescription> sortedBySize = new ArrayList<>(graphs);
		Collections.sort(sortedBySize, (o1, o2) -> {
			if (o1 == null && o2 == null)
				return 0;
			else if (o1 == null)
				return 1;
			else if (o2 == null)
				return -1;
			else
				//We want the big graphs first.
				return Long.compare(o2.getTripleCount(), o1.getTripleCount());
		});
		return sortedBySize;
	}

	public void putGraphDescription(GraphDescription gd)
	{
		assert gd != null;
		gds.put(gd.getGraphName(), gd);
	}

	public long getTotalTripleCount()
	{
		return totalTripleCount;
	}

	public void setTotalTripleCount(long totalTripleCount)
	{
		this.totalTripleCount = totalTripleCount;
	}

	public String getRelease()
	{
		return release;
	}

	public void setRelease(String release)
	{
		this.release = release;
	}

	public LocalDate getReleaseDate()
	{
		return releaseDate;
	}

	public void setReleaseDate(LocalDate releaseDate)
	{
		this.releaseDate = releaseDate;
	}


	public void deleteGraph(String graphName)
	{
		gds.remove(graphName);
	}

	public String getTitle()
	{
		return title;
	}

	public void setTitle(String title)
	{
		this.title = title;
	}

	public void setDistinctBnodeObjectCount(Long count)
	{
		this.distinctBnodeObjectCount = count;
	}

	public long getDistinctBnodeObjectCount()
	{
		return this.distinctBnodeObjectCount;
	}

	public void setDistinctIriObjectCount(long distinctIriObjectCount)
	{
		this.distinctIriObjectCount = distinctIriObjectCount;
	}

	public long getDistinctIriObjectCount()
	{
		return distinctIriObjectCount;
	}

	public void setDistinctLiteralObjectCount(Long count)
	{
		this.distinctLiteralObjectCount = count;
	}

	public long getDistinctLiteralObjectCount()
	{
		return this.distinctLiteralObjectCount;
	}

	public long getDistinctObjectCount()
	{
		return distinctIriObjectCount + distinctBnodeObjectCount + distinctLiteralObjectCount;
	}

	public void setDistinctBnodeSubjectCount(long distinctBnodeSubjectCount)
	{
		this.distinctBnodeSubjectCount = distinctBnodeSubjectCount;
	}

	public long getDistinctIriBnodeCount()
	{
		return this.distinctBnodeSubjectCount;
	}

	public void setDistinctIriSubjectCount(long distinctIriSubjectCount)
	{
		this.distinctIriSubjectCount = distinctIriSubjectCount;
	}

	public long getDistinctIriSubjectCount()
	{
		return this.distinctIriSubjectCount;
	}

	
}
