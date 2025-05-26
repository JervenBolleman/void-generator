package swiss.sib.swissprot.voidcounter.sparql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;

import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.PredicatePartition;

public class FindDataTypeIfNoClassOrDtKnownTest extends CommonTest {

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void empty(OptimizeFor of) throws IOException {
		var cv = createCommonVariables();
		var source = new ClassPartition(RDF.LIST);
		var pre = new PredicatePartition(RDF.PREDICATE);
		var counter = new FindDataTypeIfNoClassOrDtKnown(cv.with(emptyGd), pre, source, of);
		assertNull(counter.call());
		assertEquals(1, cv.finishedQueries().get());
	}
}
