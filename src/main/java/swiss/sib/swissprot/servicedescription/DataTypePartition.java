package swiss.sib.swissprot.servicedescription;

import org.eclipse.rdf4j.model.IRI;


public class DataTypePartition
{

	private IRI datatype;

	public DataTypePartition(IRI datatype)
	{
		this.datatype = datatype;
	}

	public DataTypePartition()
	{
	}

	public IRI getDatatype()
	{
		return datatype;
	}

	public void setDatatype(IRI datatype)
	{
		this.datatype = datatype;
	}
}
