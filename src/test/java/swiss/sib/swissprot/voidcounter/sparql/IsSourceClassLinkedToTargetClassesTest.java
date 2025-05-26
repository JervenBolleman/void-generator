package swiss.sib.swissprot.voidcounter.sparql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.List;

import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.PredicatePartition;

public class IsSourceClassLinkedToTargetClassesTest extends CommonTest {

    @ParameterizedTest
    @EnumSource(OptimizeFor.class)
    void empty(OptimizeFor of) throws IOException {
        var cv = createCommonVariables();
        var targets = List.of(new ClassPartition(RDF.TYPE));
        var source = new ClassPartition(RDF.LIST);
        var pre = new PredicatePartition(RDF.PREDICATE);
        var counter = new IsSourceClassLinkedToTargetClasses(cv.with(emptyGd), targets,
        		pre, source, of);

        assertNull(counter.call());
 	      assertEquals(1, cv.finishedQueries().get());
    }
}
