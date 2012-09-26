package play.modules.cassandra;

import java.util.ArrayList;
import java.util.List;

public class CassandraQuery<T extends Model> {
    private List<T> _results;

    public CassandraQuery(List<T> models) {
        _results = models;
    }

    public List<T> fetch() {
        if ( _results.isEmpty() ) {
            return new ArrayList<T>();
        }
        return fetch(_results.size());
    }

    public List<T> fetch(int max) {
        if ( _results.isEmpty() ) {
            return new ArrayList<T>();
        }
        return  _results.subList(0, Math.max(_results.size(), max));
    }

    public T first() {
        if ( _results.isEmpty()) {
            return null;
        }
        return _results.get(0);
    }
}
