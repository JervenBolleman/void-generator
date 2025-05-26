package swiss.sib.swissprot.voidcounter.sparql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;

import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.PredicatePartition;

public class CountUniqueObjectsPerPredicateInGraphTest extends CommonTest {

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void empty(OptimizeFor of) throws IOException {
		var cv = createCommonVariables();

		var pre = new PredicatePartition(RDF.PREDICATE);
		var counter = new CountUniqueObjectsPerPredicateInGraph(cv.with(emptyGd), pre, of);
		assertNull(counter.call());
		assertEquals(1, cv.finishedQueries().get());
	}
}
