package play.modules.cassandra.providers;

import com.google.gson.Gson;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import org.joda.time.DateTime;
import play.Logger;
import play.Play;
import play.db.DB;
import play.exceptions.UnexpectedException;
import play.modules.cassandra.*;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

public class H2Provider implements CassandraDB {
    private static CassandraDB _instance = null;
    private DataSource _ds = null;
    public static CassandraDB instance() {
        if ( null == _instance ) {
            _instance = new H2Provider();
        }
        return _instance;
    }

    H2Provider() {
        _ds = DB.datasource;
    }

    /**
     * No keyspace in H2 - consumer needs to know how to handle
     * null return from this function
     *
     * @return
     */
    public Keyspace getRawKeyspace() {
        return null;
    }

    private Connection getConnection() {
        try {
            Connection connection = _ds.getConnection();
            connection.setAutoCommit(true);
            return connection;
        } catch (SQLException e) {
            throw new UnexpectedException(e);
        }
    }

    private void closeConnection(Connection connection) {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new UnexpectedException(e);
        }
    }

    public void delete(Class<?> clazz, ColumnFamily<String,String> cf, Object key)
    {
        Connection conn = getConnection();
        String conditions = "";
        ModelReflector reflector = ModelReflector.reflectorFor(clazz);
        if ( null != key && Model.class.isAssignableFrom(clazz) ) {
            conditions = String.format(" WHERE `%s`='%s'", reflector.getIdentityField(), key.toString());
        }
        try {
            assertTable(cf.getName());
            conn.createStatement().execute(String.format("DELETE FROM `%s`%s;", cf.getName(), conditions));
            conn.commit();
            closeConnection(conn);
        } catch (SQLException e) {
            throw new UnexpectedException(e);
        } finally {
            closeConnection(conn);
        }

        // Delete
    }

    @Override
    public void deleteAll(ColumnFamily<String, String> cf) {
        Class<?> clazz = ModelReflector.reflectorFor(cf.getName()).getType();
        delete(clazz, cf, null);
    }

    private ResultSet execute(Connection connection, String stmt) {
        try {
            ResultSet rs = connection.createStatement().executeQuery(stmt);
            return rs;
        } catch (SQLException e) {
            throw new UnexpectedException(e);
        }
    }

    public List<? extends Model> findByKey(Class<? extends play.db.Model> clazz, ColumnFamily<String, String> cf, String key, Object value) {
        List<Model> models = new ArrayList<Model>();
        Connection conn = getConnection();
        String table = clazz.getSimpleName();
        try {
            assertTable(table);
            String stmt = String.format("SELECT * FROM `%s` WHERE %s = ?;", table, key);
            PreparedStatement pStmt = conn.prepareStatement(stmt);
            pStmt.setString(1, value.toString());
            ResultSet rs = pStmt.executeQuery();
            while ( rs.next() ) {
                Model model = parseRow(clazz, rs);
                models.add(model);
            }
        } catch (Exception e) {
            throw new UnexpectedException(e);
        } finally {
            closeConnection(conn);
        }
        return models;
    }

    public Model find(Class<? extends play.db.Model> clazz, ColumnFamily<String, String> cf, Object id) {
        Model model = null;
        ModelReflector reflector = ModelReflector.reflectorFor(clazz);
        if ( null == id ) {
            return null;
        }
        Connection conn = getConnection();
        String table = clazz.getSimpleName();
        String idField = reflector.getIdentityField();
        try {
            assertTable(table);
            String stmt = String.format("SELECT * FROM `%s` WHERE %s = ?;", table, idField);
            PreparedStatement pStmt = conn.prepareStatement(stmt);
            pStmt.setString(1, id.toString());
            ResultSet rs = pStmt.executeQuery();
            if ( rs.next() ) {
                model = parseRow(clazz, rs);
            }
        } catch (Exception e) {
            throw new UnexpectedException(e);
        } finally {
            closeConnection(conn);
        }
        return model;
    }

    private <T extends play.db.Model> T parseRow(Class<? extends play.db.Model> clazz, ResultSet rs) {
        try {
            T model = (T) clazz.newInstance();
            ModelReflector reflector = ModelReflector.reflectorFor(clazz);
            String modelId = rs.getString(reflector.getIdentityField());
            for (ColumnField field : reflector.getColumnFields()) {
                Object value;
                String sValue = rs.getString(field.getName());
                Class objClazz = field.getType();
                if (sValue == null) {
                    if (!field.allowNull()) {
                        throw new UnexpectedException(clazz.getName() + "." + field.getName() + " specified as NotNull but DB value is null!");
                    }
                }
                if ( play.db.Model.class.isAssignableFrom(objClazz) ) {
                    String vType = objClazz.getName();
                    if (!vType.startsWith("models.")) {
                        vType = "models." + vType;
                    }
                    Class<? extends play.db.Model> vClazz = (Class<? extends play.db.Model>)Play.classloader.loadClass(vType);

                    value = play.db.Model.Manager.factoryFor(vClazz).findById(sValue);
                } else if ( field.getType().isAssignableFrom(Boolean.TYPE) || field.getType().isAssignableFrom(Boolean.class)) {
                    value = (sValue == null) ? false : Boolean.parseBoolean(sValue);
                } else if ( field.getType().isAssignableFrom(Long.TYPE) || field.getType().isAssignableFrom(Long.class)) {
                    value = (sValue == null) ? 0L : Long.parseLong(sValue);
                } else if ( field.getType().isAssignableFrom(Integer.TYPE) || field.getType().isAssignableFrom(Integer.class)) {
                    value = (sValue == null) ? 0 : Integer.parseInt(sValue);
                } else if (BigDecimal.class.isAssignableFrom(field.getType())) {
                    if ( null != sValue ) {
                        value = new BigDecimal(sValue);
                    } else {
                        value = null;
                    }
                } else if ( field.isList()) {
                    if ( MapModel.class.isAssignableFrom(field.genericType())) {
                        Class<? extends MapModel> mapClazz = (Class<? extends MapModel>)field.genericType();
                        List<? extends MapModel> mapModels = findComposites(mapClazz, modelId);
                        value = mapModels;
                    } else {
                        Class<?> listClass = field.genericType();
                        RelationList list = new RelationList();
                        String [] ids = new Gson().fromJson(sValue, String[].class);
                        for ( String id : ids ) {
                            list.add(new Relation(listClass.getName(), id));
                        }
                        value = list;
                    }
                } else if (DateTime.class.isAssignableFrom(field.getType())) {
                    value = (sValue == null) ? new DateTime(0L) : DateTime.parse(sValue);
                } else if ( objClazz.isEnum() ) {
                    if ( null != sValue && !sValue.toString().isEmpty() ) {
                        try {
                            field.set(model, Enum.valueOf((Class<Enum>) field.getType(), sValue));
                        } catch ( IllegalArgumentException e ) {
                        }
                    }
                    continue;
                } else {
                    value = sValue;
                }

                field.set(model, value);
            }
            ((Model)model).onLoad();
            return model;
        } catch (Exception e) {
            throw new UnexpectedException(e);
        }
    }

    public List<? extends play.db.Model> all(Class<? extends play.db.Model> clazz) {
        List<Model> models = new ArrayList<Model>();

        Connection conn = getConnection();
        String table = clazz.getSimpleName();
        this.assertTable(table);
        try {
            String stmt = String.format("SELECT * FROM `%s`;", table);
            ResultSet rs = conn.createStatement().executeQuery(stmt);
            while ( rs.next() ) {
                Model model = parseRow(clazz, rs);
                if ( null != model ) {
                    models.add(model);
                }
            }
        } catch (Exception e) {
            throw new UnexpectedException(e);
        } finally {
            closeConnection(conn);
        }

        return models;
    }

    public void save(Model o, ColumnFamily<String, String> cf) {
        save(o, cf, false);
    }

    public void save(Model o, ColumnFamily<String, String> cf, boolean saveCounters) {
        Connection conn = getConnection();
        ModelReflector reflector = ModelReflector.reflectorFor(o.getClass());
        Set<ColumnField> columnFields = reflector.getColumnFields();
        try {
            assertTable(o.getClass());
            String table = o.getClass().getSimpleName();
            String idFieldName = reflector.getIdentityField();
            PreparedStatement pstmt;
            if ( exists(o.getClass(), o.getId().toString())) {
                int fieldCount = 0;
                StringBuilder stmt = new StringBuilder(String.format("UPDATE `%s` SET ", table));
                for ( ColumnField field : columnFields) {
                    if ( field.isIdentity() ) continue;
                    if ( field.isList() && MapModel.class.isAssignableFrom(field.genericType())) {
                        continue;
                    }
                    if ( saveCounters || !field.isCounter()) {
                        if ( fieldCount > 0 ) {
                            stmt.append(", ");
                        }
                        stmt.append(String.format("`%s`=?", field.getName()));
                        fieldCount++;
                    }
                }
                stmt.append(String.format(" WHERE %s = ?;", idFieldName));
                pstmt = conn.prepareStatement(stmt.toString());
                fieldCount=0;
                ColumnField idField=null;
                for ( ColumnField field : columnFields) {
                    if ( field.isIdentity()) {
                        idField = field;
                        continue;
                    }

                    if ( saveCounters || !field.isCounter()) {
                        Class objClazz = field.getType();

                        if ( null == field.get(o)) {
                            pstmt.setNull(++fieldCount, Types.VARCHAR);
                        } else if ( field.isList() ) {
                            if ( MapModel.class.isAssignableFrom(field.genericType())) {
                                List<MapModel> mapModels = (List<MapModel>)field.get(o);
                                for ( MapModel model : mapModels ) {
                                    if ( model.isChanged() ) {
                                        writeComposite(model);
                                    }
                                }
                            } else {
                                List<String> idList = new ArrayList<String>();
                                List<play.db.Model> relations = (List<play.db.Model>)field.get(o);
                                if ( null != relations ) {
                                    for ( play.db.Model model : relations ) {
                                        if ( null != model && null != model._key() ) {
                                            idList.add(model._key().toString());
                                        }
                                    }
                                }
                                pstmt.setString(++fieldCount, new Gson().toJson(idList));
                            }
                        } else if ( play.db.Model.class.isAssignableFrom(objClazz) ) {
                            play.db.Model model = (play.db.Model)field.get(o);
                            pstmt.setString(++fieldCount, model._key().toString());
                        } else {
                            pstmt.setString(++fieldCount, field.get(o).toString());
                        }
                    }
                }
                if ( null == idField) {
                    throw new UnexpectedException("No ID field found for model");
                }
                pstmt.setString(++fieldCount, idField.get(o).toString());
            } else {
                StringBuilder columns = new StringBuilder();
                StringBuilder values = new StringBuilder();
                int fieldCount = 0;
                for ( ColumnField field : columnFields) {
                    if ( fieldCount > 0 ) {
                        columns.append(", ");
                        values.append(",");
                    }

                    columns.append(String.format("`%s`", field.getName()));
                    values.append("?");
                    fieldCount++;
                }
                String stmt = String.format("INSERT INTO `%s` ( %s ) VALUES ( %s );", table, columns, values);
                fieldCount = 0;
                conn.setAutoCommit(false);
                pstmt = conn.prepareStatement(stmt);
                for ( ColumnField field : columnFields) {
                    Class objClazz = field.getType();
                    if ( !Collection.class.isAssignableFrom(objClazz) && null == field.get(o)) {
                        pstmt.setNull(++fieldCount, Types.VARCHAR);
                    } else if ( Collection.class.isAssignableFrom(objClazz) ) {
                        List<String> idList = new ArrayList<String>();
                        List<play.db.Model> relations = (List<play.db.Model>)field.get(o);
                        if ( null != relations ) {
                            for ( play.db.Model model : relations ) {
                                if ( null != model && null != model._key() ) {
                                    idList.add(model._key().toString());
                                }
                            }
                        }
                        pstmt.setString(++fieldCount, new Gson().toJson(idList));
                    } else if ( play.db.Model.class.isAssignableFrom(objClazz) ) {
                        play.db.Model model = (play.db.Model)field.get(o);
                        pstmt.setString(++fieldCount, model._key().toString());
                    } else {
                        pstmt.setString(++fieldCount, field.get(o).toString());
                    }
                }
            }

            if ( null == pstmt ) {
                throw new UnexpectedException("Failed to create a prepared statement");
            }
            pstmt.execute();
            conn.commit();
        } catch ( Exception e ) {
            CassandraLogger.error(e, "Failed to save SQL model");
            throw new UnexpectedException(e);
        } finally {
            closeConnection(conn);
        }
    }

    public long count(ColumnFamily<String, String> cf) {
        long results = 0;
        Connection conn = getConnection();
        ResultSet rs = null;
        try {
            assertTable(cf.getName());
            String stmt = String.format("SELECT COUNT(1) as count FROM `%s`;", cf.getName());
            rs = execute(conn, stmt);
            if ( rs.next() ) {
                results = rs.getInt("count");
            }
        } catch ( Exception e ) {
            throw new UnexpectedException(e);
        } finally {
            if ( null != rs ) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    throw new UnexpectedException(e);
                }
            }
            closeConnection(conn);
        }
        return results;
    }

    public boolean increment(String id, String cfName, String columnName, long amount) {
        String vType = cfName;
        ModelReflector reflector = ModelReflector.reflectorFor(cfName);
        if (!vType.startsWith("models.")) {
            vType = "models." + vType;
        }
        ColumnFamily<String, String> cf = ColumnFamily.newColumnFamily(cfName, StringSerializer.get(), StringSerializer.get());
        Class<? extends Model> clazz;
        try {
            clazz = (Class<? extends Model>) Play.classloader.loadClass(vType);
        } catch (ClassNotFoundException e) {
            throw new UnexpectedException("Could not load class");
        }
        assertTable(clazz);
        String table = clazz.getSimpleName();
        Field field = reflector.getFieldByName(columnName);
        if ( null == field ) {
            throw new UnexpectedException("Unable to find increment field");
        }

        play.modules.cassandra.annotations.Counter counter = field.getAnnotation(play.modules.cassandra.annotations.Counter.class);
        if ( null == counter ) {
            throw new UnexpectedException("Could not increment non-counter field");
        }

        Model model = find(clazz, cf, id);
        if ( null == model ) {
            //throw new UnexpectedException("Unable to load model " + id);
            return false;
        }

        String newValue = "0";
        if ( BigDecimal.class.isAssignableFrom(field.getType())) {
            Integer scale = Integer.parseInt(counter.scale());
            try {
                BigDecimal currValue = (BigDecimal)field.get(model);
                newValue = currValue.add(new BigDecimal(amount).divide(new BigDecimal(Math.pow(10,scale)), scale, BigDecimal.ROUND_DOWN)).toString();
            } catch ( IllegalAccessException e ) {
                CassandraLogger.warn(e, "Illegal Access Exception parsing a BigDecimal");
            }
        } else {
            try {
                Long val = (Long)field.get(model)+amount;
                newValue = val.toString();
            } catch ( IllegalAccessException e ) {
                CassandraLogger.warn(e, "Illegal Access Exception parsing a Long");
            }
        }
        String sqlStmt = String.format("UPDATE %s SET %s = ? WHERE %s = ?", table, columnName, reflector.getIdentityField());

        Connection conn = getConnection();
        try {
            PreparedStatement pstmt = conn.prepareStatement(sqlStmt);
            pstmt.setString(1, newValue);
            pstmt.setString(2, model._key().toString());
            pstmt.execute();
        } catch (SQLException e) {
            Logger.error(e, "SQL Error in H2Memory Cassandra");
        } finally {
            closeConnection(conn);
        }

        return true;
    }

    private boolean exists(Class<? extends Model> clazz, String id ) {
        ModelReflector reflector = ModelReflector.reflectorFor(clazz);
        String table = clazz.getSimpleName();
        Boolean found = false;
        Connection connection = getConnection();
        String idField = reflector.getIdentityField();
        try {
            ResultSet rs = connection.createStatement().executeQuery(String.format("SELECT * FROM `%s` WHERE `%s`='%s';", table, idField, id));
            if ( rs.next() ) {
                found = true;
            }
            rs.close();
        } catch (SQLException e ) {
            throw new UnexpectedException(e);
        } finally {
            closeConnection(connection);
        }
        return found;
    }

    private void assertTable(String className) {
        try {
            String vType = className;
            if (!vType.startsWith("models.")) {
                vType = "models." + vType;
            }
            Class<?> clazz = Play.classloader.loadClass(vType);
            assertTable(clazz);
        } catch ( Exception e ) {
            throw new UnexpectedException(e);
        }
    }

    private void assertTable(Class<?> clazz) {
        if ( play.db.Model.class.isAssignableFrom(clazz)) {
            assertPlayModelTable(clazz);
        }

        if ( MapModel.class.isAssignableFrom(clazz)) {
            assertMapModelTable(clazz);
        }
    }

    private void assertMapModelTable(Class<?> clazz) {
        Connection connection = getConnection();
        String table = clazz.getSimpleName();
        ResultSet rs = null;
        try {
            rs = connection.getMetaData().getTables(null, null, table, null);
            if ( !rs.next() ) {
                String stmt = String.format("CREATE TABLE `%s` ( rowKey varchar(2000), dictionaryKey varchar(2000), keyName varchar(2000), value varchar(2000) );", table);
                connection.createStatement().execute(stmt);
                connection.commit();
            }
        } catch (SQLException e ) {
            throw new UnexpectedException(e);
        } finally {
            if ( null != rs ) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    throw new UnexpectedException(e);
                }
            }
            closeConnection(connection);
        }
    }


    /**
     * TODO: Cache assertions, so we don't have to query the meta data each time
     *
     * @param clazz
     */
    private void assertPlayModelTable(Class<?> clazz) {
        Connection connection = getConnection();
        String table = clazz.getSimpleName();
        ResultSet rs = null;
        ModelReflector reflector = ModelReflector.reflectorFor(clazz);
        try {
            rs = connection.getMetaData().getTables(null, null, table, null);
            if ( !rs.next() ) {
                StringBuilder stmt = new StringBuilder(String.format("CREATE TABLE `%s` ( ", table));
                int fieldCount = 0;
                for ( ColumnField field : reflector.getColumnFields() ) {
                    if ( fieldCount > 0 ) {
                        stmt.append(",");
                    }
                    if ( field.isIdentity()) {
                        stmt.append(String.format("`%s` varchar(255) not null primary key", field.getName()));
                    } else if ( field.isCounter() || field.isIndex() || field.isList() ) {
                        stmt.append(String.format("`%s` varchar(2000)", field.getName()));
                    } else {
                        stmt.append(String.format("`%s` text", field.getName()));
                    }
                    fieldCount++;
                }
                stmt.append(");");
                connection.createStatement().execute(stmt.toString());
                connection.commit();
            }
        } catch (SQLException e ) {
            throw new UnexpectedException(e);
        } finally {
            if ( null != rs ) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    throw new UnexpectedException(e);
                }
            }
            closeConnection(connection);
        }
    }

    public <T> T getColumnValue(String cfName, String id, String columnName) {
        try {
            String vType = cfName;
            if (!vType.startsWith("models.")) {
                vType = "models." + vType;
            }
            ColumnFamily<String, String> cf = ColumnFamily.newColumnFamily(cfName, StringSerializer.get(), StringSerializer.get());
            Class<? extends Model> clazz = (Class<? extends Model>) Play.classloader.loadClass(vType);
            ModelReflector reflector = ModelReflector.reflectorFor(clazz);
            Model o = find(clazz, cf, id);
            if ( null != o) {
                for ( ColumnField field : reflector.getColumnFields()) {
                    if ( field.getName().equals(columnName) ) {
                        return (T)field.get(o);
                    }
                }
            }
        } catch (Exception e) {
            throw new UnexpectedException(e);
        }
        return null;
    }

    public void setColumnValue(String cfName, String id, String columnName, Object value) {
        try {
            String vType = cfName;
            if (!vType.startsWith("models.")) {
                vType = "models." + vType;
            }
            ColumnFamily<String, String> cf = ColumnFamily.newColumnFamily(cfName, StringSerializer.get(), StringSerializer.get());
            Class<? extends Model> clazz = (Class<? extends Model>) Play.classloader.loadClass(vType);
            ModelReflector reflector = ModelReflector.reflectorFor(clazz);
            Model o = find(clazz, cf, id);
            if ( null != o) {
                for ( ColumnField field : reflector.getColumnFields()) {
                    if ( field.getName().equals(columnName) ) {
                        field.set(o, value);
                        save(o, cf, true);
                        return;
                    }
                }
            }
        } catch (Exception e) {
            throw new UnexpectedException(e);
        }
    }

    // TODO: Perform this function in a single mutation
    public void writeComposite(MapModel model) {
        ModelReflector reflector = ModelReflector.reflectorFor(model.getClass());
        // TODO: Figure out this error
        if ( null == reflector ) return;
        ColumnFamily<String, String> cf = reflector.getColumnFamily();
        String cfName = cf.getName();
        for ( String key : model.keySet() ) {
            writeComposite(cfName, model.getKey1(), model.getKey2(), key, model.get(key), null);
        }
    }


    public void writeComposite(String cfName, String rowKey, String dictionaryKey, String key, String value, Integer ttl ) {
        deleteComposite(cfName,  rowKey, dictionaryKey, key);

        Connection conn = getConnection();
        try {
            assertTable(cfName);
            String stmt = String.format("INSERT INTO `%s` ( rowKey, dictionaryKey, keyName, value ) VALUES ( ?, ?, ?, ? );", cfName);
            PreparedStatement pstmt = conn.prepareStatement(stmt);
            pstmt.setString(1, rowKey);
            pstmt.setString(2, dictionaryKey);
            pstmt.setString(3, key);
            pstmt.setString(4, value);
            pstmt.execute();
            conn.commit();
        } catch ( Exception e ) {
            throw new UnexpectedException(e);
        } finally {
            closeConnection(conn);
        }
    }

    public Map<String, String> findComposite(String cfName, String key, String subkey) {
        HashMap<String, String> map = new HashMap<String, String>();
        Connection conn = getConnection();
        try {
            assertTable(cfName);
            String stmt = String.format("SELECT keyName, value FROM `%s`  WHERE rowKey = ? AND dictionaryKey = ?;", cfName);
            PreparedStatement pstmt = conn.prepareStatement(stmt);
            pstmt.setString(1, key);
            pstmt.setString(2, subkey);
            ResultSet rs = pstmt.executeQuery();
            while ( rs.next() ) {
                map.put(rs.getString(1), rs.getString(2));
            }
            conn.commit();
        } catch ( Exception e ) {
            throw new UnexpectedException(e);
        } finally {
            closeConnection(conn);
        }
        return map;
    }

    public int deleteComposite(String cfName, String key, String dictionary, String dictKey) {
        assertTable(cfName);

        int rowsAffected = 0;
        Connection conn = getConnection();
        try {
            PreparedStatement pstmt;
            if ( null != dictKey) {
                pstmt = conn.prepareStatement(String.format("DELETE FROM `%s` WHERE rowKey = ? AND dictionaryKey = ? AND keyName = ?;", cfName));
                pstmt.setString(1, key);
                pstmt.setString(2, dictionary);
                pstmt.setString(3, dictKey);
            } else {
                pstmt = conn.prepareStatement(String.format("DELETE FROM `%s` WHERE rowKey = ? AND dictionaryKey = ?;", cfName));
                pstmt.setString(1, key);
                pstmt.setString(2, dictionary);
            }
            rowsAffected = pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new UnexpectedException(e);
        } finally {
            closeConnection(conn);
        }
        return rowsAffected;
    }

    public List<? extends MapModel> findComposites(Class<? extends MapModel> clazz, String key) {
        String cfName = ModelReflector.reflectorFor(clazz).getColumnFamily().getName();
        return findComposites(cfName, key, clazz);
    }

    public <T extends MapModel> List<T> findComposites(String cfName, String key, Class<? extends MapModel> modelClazz) {
        List<T> models = new ArrayList<T>();
        HashMap<String, T> modelMap = new HashMap<String, T>();
        Connection conn = getConnection();
        try {
            assertTable(cfName);
            String stmt = String.format("SELECT rowKey, dictionaryKey, keyName, value FROM `%s` WHERE rowKey = ?;", cfName);
            PreparedStatement pstmt = conn.prepareStatement(stmt);
            pstmt.setString(1, key);
            ResultSet rs = pstmt.executeQuery();
            while ( rs.next() ) {
                String rowKey = rs.getString(1);
                String dictionaryKey = rs.getString(2);
                String keyName = rs.getString(3);
                String value = rs.getString(4);
                T model = modelMap.get(dictionaryKey);
                if ( null == model ) {
                    model = (T) modelClazz.newInstance();
                    model.setKey1(rowKey);
                    model.setKey2(dictionaryKey);
                }
                model.put(keyName,value);
                modelMap.put(dictionaryKey, model);
            }
            conn.commit();
        } catch ( Exception e ) {
            throw new UnexpectedException(e);
        } finally {
            closeConnection(conn);
        }
        for ( T model : modelMap.values()) {
            models.add(model);
        }
        return  models;
    }

    /**
     * Informational functions
     */
    @Override
    public String getProviderType() {
        return this.getClass().getSimpleName();
    }

    @Override
    public String getHosts() {
        return "  Database resides in-memory";
    }
}
