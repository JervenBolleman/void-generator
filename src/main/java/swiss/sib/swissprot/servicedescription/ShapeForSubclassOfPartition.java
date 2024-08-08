package swiss.sib.swissprot.servicedescription;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.RDFS;



public class ShapeForSubclassOfPartition
    extends ShapePartition
{
	private final IRI superClass;
	
	public ShapeForSubclassOfPartition(IRI superClass) {
		this.superClass = superClass;
	}

	@Override
	public String getShapeMembersSelectionQuery() {
		return "?thing <"+RDFS.SUBCLASSOF+"> <"+superClass.stringValue()+"> .";
	}
}
