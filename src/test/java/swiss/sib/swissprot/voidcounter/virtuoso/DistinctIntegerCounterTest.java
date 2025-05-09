package swiss.sib.swissprot.voidcounter.virtuoso;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DistinctIntegerCounterTest {

    private DistinctIntegerCounter counter;

    @BeforeEach
    public void setup() {
        counter = new DistinctIntegerCounter();
    }

    @Test
    public void testAddAndCardinality() {
        counter.add(1);
        counter.add(2);
        counter.add(3);
        assertEquals(3, counter.cardinality());

        counter.add(2);
        assertEquals(3, counter.cardinality());

        counter.add(4);
        assertEquals(4, counter.cardinality());
    }
}
