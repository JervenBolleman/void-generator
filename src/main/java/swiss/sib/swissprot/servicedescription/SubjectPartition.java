package swiss.sib.swissprot.servicedescription;

import org.eclipse.rdf4j.model.IRI;

public class SubjectPartition {

	private IRI subject;
	private long count;

	public SubjectPartition(IRI clazz) {
		this.subject = clazz;
	}

	public SubjectPartition() {
	}

	public IRI getSubject() {
		return subject;
	}

	public void setSubject(IRI normalize) {
		this.subject = normalize;
	}

	public long getTripleCount() {
		return count;
	}

	public void setTripleCount(long count) {
		this.count = count;
	}

}
