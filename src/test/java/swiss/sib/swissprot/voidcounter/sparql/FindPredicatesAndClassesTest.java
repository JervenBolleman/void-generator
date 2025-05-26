package swiss.sib.swissprot.voidcounter.sparql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.Set;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import swiss.sib.swissprot.servicedescription.OptimizeFor;

public class FindPredicatesAndClassesTest extends CommonTest {

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void empty(OptimizeFor of) throws IOException {
		var cv = createCommonVariables();
		var counters = new SparqlCounters(of, schedule);
		var counter = new FindPredicatesAndClasses(cv.with(emptyGd), Set.of(), null, counters);
		assertNull(counter.call());
		assertEquals(1, cv.finishedQueries().get());
	}
}
