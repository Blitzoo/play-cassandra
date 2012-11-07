import models.Account;
import models.Order;
import models.Room;
import org.junit.Before;
import org.junit.Test;
import play.test.Fixtures;
import play.test.UnitTest;

public class FixturesTest extends UnitTest {
    @Before
    public void setup() {
        // Refresh database prior to each test
        Fixtures.deleteAllModels();
        Fixtures.loadModels("initial-data.yml");
    }

    @Test
    public void testReadRelatedCassandraModelFromFixtures() {
        Order order = Order.findById("testfind");
        assertNotNull(order);
        assertEquals("fine", order.receiptStatus);
        assertNotNull(order.user);
        assertEquals("5b74e588-2653-4ddb-939e-f03045b214ee", order.user.guid);

        order = Order.findById("fakeorderid");
        assertNull(order);
    }

    @Test
    public void testReadRelatedCassandraModelFromDB() {
        Order order = new Order();
        order.id = "yadabada";

        Account account = Account.findById("5b74e588-2653-4ddb-939e-f03045b214ee");
        assertNotNull(account);

        order.user = account;
        order.save();

        Order validate = Order.findById("yadabada");
        assertNotNull(validate);
        assertNotNull(order.user);
        assertEquals("5b74e588-2653-4ddb-939e-f03045b214ee", order.user.guid);
    }

    @Test
    public void testDeleteFixtures() {
        Long expected = 2L;
        Long actual = Order.count();

        assertEquals(expected, actual);

        Order model = Order.findById("testfind");
        assertNotNull(model);

        Fixtures.deleteAllModels();

        expected = 0L;
        actual = Order.count();

        assertEquals(expected, actual);

        // Attempt to query this model
        // Since Cassandra doesn't delete the data immediately, this
        // ensures the client is able to identify the difference
        // between real and tombstone data
        model = Order.findById("testfind");
        assertNull(model);
    }

    @Test
    public void testReadRelatedCassandraModelListFromFixtures() {
        Room room = Room.findById("garber");
        assertNotNull(room);
        assertNotNull(room.games);
        assertEquals(2, room.games.size());
    }
}
