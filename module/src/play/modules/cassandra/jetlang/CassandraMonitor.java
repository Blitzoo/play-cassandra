package play.modules.cassandra.jetlang;

import com.netflix.astyanax.model.ColumnFamily;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.core.Callback;
import org.jetlang.core.Disposable;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import play.modules.cassandra.CassandraDB;
import play.modules.cassandra.Model;
import play.modules.cassandra.ModelReflector;

/**
 * This class implements JetLang monitors for the parallelization of Cassandra reads
 * and writes.
 *
 * User: mwilson
 * Date: 12-05-11
 * Time: 1:54 PM
 */
public class CassandraMonitor implements Disposable {
    private Fiber _writeFiber;
    private Channel<WriteEvent> _writeChannel;
    private Channel<play.modules.cassandra.jetlang.DeleteEvent> _deleteChannel;
    private Fiber _deleteFiber;
    private CassandraDB _ds;

    public CassandraMonitor(CassandraDB ds) {
        _ds = ds;

        _deleteFiber = new ThreadFiber();
        _deleteFiber.start();
        _writeFiber = new ThreadFiber();
        _writeFiber.start();

        _writeChannel = new MemoryChannel<WriteEvent>();
        _deleteChannel = new MemoryChannel<play.modules.cassandra.jetlang.DeleteEvent>();

        Callback<DeleteEvent> onDeleteMsg = new Callback<play.modules.cassandra.jetlang.DeleteEvent>() {
            @Override
            public void onMessage(play.modules.cassandra.jetlang.DeleteEvent deleteEvent) {
                _ds.deleteAll(deleteEvent.cf);
            }
        };

        Callback<play.modules.cassandra.jetlang.WriteEvent> onMsg = new Callback<play.modules.cassandra.jetlang.WriteEvent>() {
            @Override
            public void onMessage(play.modules.cassandra.jetlang.WriteEvent writeEvent) {
                ModelReflector reflector = ModelReflector.reflectorFor(writeEvent.model.getClass());

                // Cassandra
                _ds.save(writeEvent.model, reflector.getColumnFamily(), writeEvent.saveCounters);
            }
        };

        _writeChannel.subscribe(_writeFiber, onMsg);
        _deleteChannel.subscribe(_deleteFiber, onDeleteMsg);
    }

    public void publishDelete(ColumnFamily<String, String> cf) {
        play.modules.cassandra.jetlang.DeleteEvent deleteEvent = new play.modules.cassandra.jetlang.DeleteEvent();
        deleteEvent.cf = cf;
        _deleteChannel.publish(deleteEvent);
    }

    public void publishSave(Model o, Boolean saveCounters) {
        play.modules.cassandra.jetlang.WriteEvent event = new play.modules.cassandra.jetlang.WriteEvent();
        event.model = o;
        event.saveCounters = saveCounters;
        _writeChannel.publish(event);
    }

    @Override
    public void dispose() {
        _deleteFiber.dispose();
        _deleteFiber = null;

        _writeFiber.dispose();
        _writeFiber = null;
    }
}
