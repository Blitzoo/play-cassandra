import models.Order;
import org.junit.Before;
import org.junit.Test;
import play.test.Fixtures;
import play.test.UnitTest;

import java.math.BigDecimal;

public class CounterTest extends UnitTest {
    @Before
    public void setup() {
        // Refresh database prior to each test
        Fixtures.deleteAllModels();
        Fixtures.loadModels("initial-data.yml");
    }

    @Test
    public void testCreateWithCounters() {
        String orderId = "asjofgiesaijogas";
        Order model = new Order();
        model.id = orderId;
        model.quantity = 15;
        model.save(true);

        Long quantity = Order.get(orderId, "quantity");
        assertEquals((Long) 15L, quantity);
    }

    /**
     * What happens when Cassandra cluster is down?
     * What happens when the model is not yet consistent (slow update?)
     * What happens when this number is changed by a save (e.g. a levelup) prior to committing?
     */
    @Test
    public void testIncrement() {
        // If invalid field, this should fail
        boolean result  = Order.increment("testfind", "coffeecups", 5);
        assertFalse(result);

        Long origQuantity = Order.get("testfind", "QUANTITY");
        assertEquals((Long) 5L, origQuantity);

        result = Order.increment("testfind", "QUANTITY", 7);
        assertTrue(result);

        Order model = Order.findById("testfind");
        assertNotNull(model);
        assertEquals(12, model.quantity);

        // Ensure that changing the incremented column does not
        // result in a commit to the Counters namespace
        model.quantity = 3;
        model.save();

        Order nonIncrementedModel = Order.findById("testfind");
        assertNotNull(nonIncrementedModel);
        assertEquals(12, nonIncrementedModel.quantity);
    }

    @Test
    public void testDecimalIncrement_withLong() {
        Order model = Order.findById("testfind");
        assertNotNull(model);
        model.refund = new BigDecimal("0");
        model.save(true);

        Order.increment("testfind", "refund", 15);

        model = Order.findById("testfind");

        BigDecimal expected = new BigDecimal("0.15");
        assertEquals(expected, model.refund);

        Order.increment("testfind", "refund", 15);

        model = Order.findById("testfind");

        expected = new BigDecimal("0.30");
        assertEquals(expected, model.refund);
    }

    @Test
    public void testDecimalIncrement_withDecimal() {
        Order model = Order.findById("testfind");
        assertNotNull(model);
        model.refund = new BigDecimal("0");
        model.save(true);

        Order.increment("testfind", "refund", new BigDecimal("15.00"));

        model = Order.findById("testfind");

        BigDecimal expected = new BigDecimal("15.00");
        assertEquals(expected, model.refund);

        Order.increment("testfind", "refund", new BigDecimal("15.00"));

        model = Order.findById("testfind");

        expected = new BigDecimal("30.00");
        assertEquals(expected, model.refund);
    }
}
