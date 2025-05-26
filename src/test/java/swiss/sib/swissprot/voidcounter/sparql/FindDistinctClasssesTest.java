package swiss.sib.swissprot.voidcounter.sparql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import swiss.sib.swissprot.servicedescription.OptimizeFor;

public class FindDistinctClasssesTest extends CommonTest {

	@ParameterizedTest
	@EnumSource(OptimizeFor.class)
	void empty(OptimizeFor of) throws IOException {
		var cv = createCommonVariables();
		var counters = new SparqlCounters(of, schedule);
		var counter = new FindDistinctClassses(cv.with(emptyGd), null, null, of, counters);
		assertNull(counter.call());
		assertEquals(1, cv.finishedQueries().get());
	}
}
