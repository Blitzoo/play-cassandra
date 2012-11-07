import models.Account;
import models.Order;
import org.junit.Before;
import org.junit.Test;
import play.Logger;
import play.test.Fixtures;
import play.test.UnitTest;

public class GetSetTest extends UnitTest {
    @Before
    public void setup() {
        // Refresh database prior to each test
        Fixtures.deleteAllModels();
        Fixtures.loadModels("initial-data.yml");

        Order order = Order.findById("testfind");
        Account account = Account.findById("5b74e588-2653-4ddb-939e-f03045b214ee");
        order.user = account;
        order.save();
    }

    @Test
    public void testGetSetColumn() {
        String id = "5b74e588-2653-4ddb-939e-f03045b214ee";
        Long expected = 1238L;
        String testId = Account.get(id, "guid");
        Long actual = Account.get(id, "xp");
        assertEquals(id, testId);
        assertEquals(expected, actual);

        Logger.trace("===========TESTGETSETCOLUMN============");
        expected = 456L;
        Account.set(id, "xp", expected);
        actual = Account.get(id, "xp");
        Logger.trace("===========TESTGETSETCOLUMN============");
        assertEquals(expected, actual);
    }
}
