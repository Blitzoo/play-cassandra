package play.modules.cassandra;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ColumnFamily;

import java.util.List;
import java.util.Map;


public interface CassandraDB {
    public void deleteAll(ColumnFamily<String, String> cf);

    public Model find(Class<? extends play.db.Model> clazz, ColumnFamily<String, String> cf, Object id);

    public List<? extends play.db.Model> all(Class<? extends play.db.Model> clazz);

    public void save(Model o, ColumnFamily<String, String> cf);

    public void save(Model o, ColumnFamily<String, String> cf, boolean saveCounters);

    public long count(ColumnFamily<String, String> cf);

    public boolean increment(String id, String cfName, String columnName, long amount);

    public <T> T getColumnValue(String cfName, String id, String columnName);

    public void setColumnValue(String cfName, String id, String columnName, Object value);

    public void delete(Class<?> clazz, ColumnFamily<String,String> cf, Object key);

    // For MapModel
    public List<? extends MapModel> findComposites(Class<? extends MapModel> clazz, String key);

    public void writeComposite(String cfName, String rowKey, String dictionaryKey, String key, String value, Integer ttl );

    public Map<String, String> findComposite(String cfName, String key, String subkey);

    public int deleteComposite(String cfName, String key, String dictionary, String dictKey);

    public List<? extends play.db.Model> findByKey(Class<? extends play.db.Model> clazz, ColumnFamily<String, String> cf, String key, Object value);

    public String getProviderType();

    String getHosts();

    Keyspace getRawKeyspace();
}
