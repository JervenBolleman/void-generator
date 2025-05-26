package swiss.sib.swissprot.voidcounter.sparql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import swiss.sib.swissprot.servicedescription.OptimizeFor;

public class CountDistinctIriObjectsInAGraphTest extends CommonTest {

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void empty(OptimizeFor of) throws IOException {
		var cv = createCommonVariables();
		var counter = new CountDistinctIriObjectsInAGraph(cv.with(emptyGd), of);
		assertNull(counter.call());
		assertEquals(1, cv.finishedQueries().get());
	}
}
