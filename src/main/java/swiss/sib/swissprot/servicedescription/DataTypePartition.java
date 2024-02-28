package swiss.sib.swissprot.servicedescription;

import org.eclipse.rdf4j.model.IRI;

public class DataTypePartition {

	private IRI datatype;
	private long count;

	public DataTypePartition(IRI datatype) {
		this.datatype = datatype;
	}

	public DataTypePartition() {
	}

	public IRI getDatatype() {
		return datatype;
	}

	public void setDatatype(IRI datatype) {
		this.datatype = datatype;
	}

	public void setTripleCount(long count) {
		this.count = count;
	}

	public long getTripleCount() {
		return count;
	}
}
