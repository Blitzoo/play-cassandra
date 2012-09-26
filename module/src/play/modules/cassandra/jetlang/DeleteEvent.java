package play.modules.cassandra.jetlang;

import com.netflix.astyanax.model.ColumnFamily;

public class DeleteEvent {
    ColumnFamily<String, String> cf;
}
