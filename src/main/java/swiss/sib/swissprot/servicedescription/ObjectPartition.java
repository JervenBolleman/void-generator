package swiss.sib.swissprot.servicedescription;

import org.eclipse.rdf4j.model.IRI;

public class ObjectPartition {

	private IRI object;
	private long count;

	public ObjectPartition(IRI clazz) {
		this.object = clazz;
	}

	public ObjectPartition() {
	}

	public IRI getObject() {
		return object;
	}

	public void setObject(IRI normalize) {
		this.object = normalize;
	}

	public long getTripleCount() {
		return count;
	}

	public void setTripleCount(long count) {
		this.count = count;
	}

}
