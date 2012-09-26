package play.modules.cassandra;

import play.Play;
import play.exceptions.UnexpectedException;
import play.modules.cassandra.providers.CassandraProvider;
import play.modules.cassandra.providers.H2Provider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Defines a mapping model (composite column)
 */
public class MapModel extends HashMap<String, String> {
    private Boolean _changed = false;
    private String _key1;
    private String _key2;
    private static CassandraDB _db=null;

    public MapModel() {
        this(null,null);
    }

    public MapModel(String key1, String key2) {
        _key1 = key1;
        _key2 = key2;
        _changed = false;
    }

    public String getKey1() { return _key1; }
    public String getKey2() { return _key2; }
    public void setKey1(String value ) { _key1 = value; }
    public void setKey2(String value ) { _key2 = value; }

    // TODO: Refactor this code - it's copied from CassandraPlugin.java
    private static CassandraDB ds() {
        Boolean useMemDb = Boolean.parseBoolean(Play.configuration.getProperty("cassandra.usememdb", "false"));
        if ( null == _db ) {
            if ( useMemDb ) {
                _db = H2Provider.instance();
            } else {

                _db = CassandraProvider.instance();
            }
        }
        return _db;
    }

    public static int delete(String rowKey, String dictionaryKey) {
        throw new UnsupportedOperationException(
                "Please annotate your model with @play.modules.cassandra.annotations.Entity annotation.");
    }

    public static int delete(String rowKey, String dictionaryKey, String columnKey) {
        throw new UnsupportedOperationException(
                "Please annotate your model with @play.modules.cassandra.annotations.Entity annotation.");
    }

    public static List<String> getDictionaryKeys(String rowkey) {
        throw new UnsupportedOperationException(
                "Please annotate your model with @play.modules.cassandra.annotations.Entity annotation.");
    }

    public static <T extends MapModel> List<T> findAll(String key) {
        throw new UnsupportedOperationException(
                "Please annotate your model with @play.modules.cassandra.annotations.Entity annotation.");
    }

    public static <T extends MapModel> T find(String rowKey, String dictionaryKey) {
        throw new UnsupportedOperationException(
                "Please annotate your model with @play.modules.cassandra.annotations.Entity annotation.");
    }

    public static void writeComposite(String rowKey, String dictionaryKey, String key, String value, Integer ttl ) {
        throw new UnsupportedOperationException(
                "Please annotate your model with @play.modules.cassandra.annotations.Entity annotation.");
    }

    public static void writeComposite(String rowKey, String dictionaryKey, String key, String value ) {
        throw new UnsupportedOperationException(
                "Please annotate your model with @play.modules.cassandra.annotations.Entity annotation.");
    }

    protected static void _writeComposite(String cfName, String rowKey, String dictionaryKey, String key, String value, Integer ttl ) {
        ds().writeComposite(cfName, rowKey, dictionaryKey, key, value,ttl);
    }

    protected static int _delete(String cfName, String rowKey, String dictionaryKey, String columnKey) {
        return ds().deleteComposite(cfName, rowKey, dictionaryKey, columnKey);
    }

    protected static List<String> _getDictionaryKeys(String cfName, String rowKey) {
        List<String> keys = new ArrayList<String>();
        List<? extends MapModel> models = _findAll(cfName, rowKey);
        for ( MapModel model : models ) {
            keys.add(model.getKey2());
        }
        return keys;
    }

    protected static List<? extends MapModel> _findAll(String cfName, String rowKey) {
        String vType = cfName;
        if (!vType.startsWith("models.")) {
            vType = "models." + vType;
        }
        try {
            Class<? extends MapModel> clazz = (Class<? extends MapModel>)Play.classloader.loadClass(vType);
            return ds().findComposites(clazz, rowKey);
        } catch (Exception e) {
            throw new UnexpectedException(e);
        }
    }

    protected static MapModel _find(String cfName, String rowKey, String dictionaryKey) {
        String vType = cfName;
        if (!vType.startsWith("models.")) {
            vType = "models." + vType;
        }
        try {
            Class<? extends MapModel> clazz = (Class<? extends MapModel>)Play.classloader.loadClass(vType);
            MapModel t = clazz.newInstance();
            t.setKey1(rowKey);
            t.setKey2(dictionaryKey);
            Map<String, String> map = ds().findComposite(cfName, rowKey, dictionaryKey);
            t.putAll(map, false);
            return t;
        } catch (Exception e) {
            throw new UnexpectedException(e);
        }
    }

    public boolean isChanged() {
        return _changed;
    }

    @Override
    public String put(String s, String s1) {
        _changed = true;
        return super.put(s, s1);
    }

    @Override
    public void clear() {
        _changed = true;
        super.clear();
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> map) {
        putAll(map, true);
    }

    /**
     * @param map Source map
     * @param setChangedStatus Set to TRUE if this should toggle the changed status
     */
    protected void putAll(Map<? extends String, ? extends String> map, boolean setChangedStatus) {
        if ( map.size() > 0 ) {
            // Do not turn off the changed boolean if it is already on
            _changed = _changed || setChangedStatus;
        }
        super.putAll(map);
    }
}