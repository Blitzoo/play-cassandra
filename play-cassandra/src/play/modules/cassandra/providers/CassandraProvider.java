package play.modules.cassandra.providers;

import com.google.gson.Gson;
import com.netflix.astyanax.*;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.*;
import com.netflix.astyanax.retry.ExponentialBackoff;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;
import org.joda.time.DateTime;
import play.Play;
import play.exceptions.DatabaseException;
import play.exceptions.UnexpectedException;
import play.modules.cassandra.*;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;

import static play.modules.cassandra.CassandraLogger.debug;


@SuppressWarnings("unchecked")
public class CassandraProvider implements CassandraDB {
	private static CassandraProvider _instance = null;
	private static ConnectionPoolConfigurationImpl _pool = null;
	private static AstyanaxConfigurationImpl _asConfig = null;
	private AstyanaxContext<Keyspace> _context = null;

    static AnnotatedCompositeSerializer<CompositeJoinOrdinals> joinSerializer
            = new AnnotatedCompositeSerializer<CompositeJoinOrdinals>(CompositeJoinOrdinals.class);

    public static CassandraProvider instance() {
		if ( null == _instance ) {
			_instance = new CassandraProvider();
		}
		return _instance;
	}

	private CassandraProvider()
	{
        String cluster = Play.configuration.getProperty("cassandra.cluster", "PlayCluster");
        String keyspace = Play.configuration.getProperty("cassandra.keyspace");
        if ( null == keyspace ) {
            throw new UnexpectedException("Missing configuration property for cassandra.keyspace");
        }
        _context = new AstyanaxContext.Builder()
                .forCluster(cluster)
                .forKeyspace(keyspace)
                .withAstyanaxConfiguration(config())
                .withConnectionPoolConfiguration(connectionPool())
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        _context.start();
    }

	private ConnectionPoolConfigurationImpl connectionPool() {
		if ( null == _pool ) {
            _pool = CassandraSharedHelpers.createConnectionPoolConfiguration();
        }
		return _pool;
	}

	private AstyanaxConfigurationImpl config() {
		if ( null == _asConfig ) {
			_asConfig = new AstyanaxConfigurationImpl()
                    .setRetryPolicy(new ExponentialBackoff(250, 4))
                    .setDefaultWriteConsistencyLevel(ConsistencyLevel.valueOf(Play.configuration.getProperty("cassandra.concurrency.write.default", "CL_QUORUM")))
                    .setDefaultReadConsistencyLevel(ConsistencyLevel.valueOf(Play.configuration.getProperty("cassandra.concurrency.read.default", "CL_QUORUM")))
                    .setDiscoveryType(NodeDiscoveryType.valueOf(Play.configuration.getProperty("cassandra.nodediscoverytype", "NONE")))
                    .setConnectionPoolType(ConnectionPoolType.valueOf(Play.configuration.getProperty("cassandra.connectionpooltype", "ROUND_ROBIN")));
		}
		return _asConfig;
    }

    /**
     * @param clazz Class type for model instantiation
     * @return List of all models in column family
     */
    public List<? extends play.db.Model> all(Class<? extends play.db.Model> clazz) {
        ColumnFamily<String, String> cf = ModelReflector.reflectorFor(clazz).getColumnFamily();
        Rows<String, String> rows = getAllRows(cf);
        List<Model> modelList = new ArrayList<Model>();
        for ( Row<String,String> row : rows ) {
            if ( ! row.getColumns().isEmpty() ) {
                Model model = parseColumns(clazz, row.getColumns(), cf.getName(), row.getKey());
                modelList.add(model);
            }
        }
        return modelList;
    }

    private Rows<String, String> getAllRows(ColumnFamily<String, String> cf) {
        try {
            Keyspace keyspace = getKeyspace();
            OperationResult<Rows<String, String>> results = keyspace.prepareQuery(cf)
                    .getAllRows()
                    .execute();

            return results.getResult();
        } catch ( ConnectionException e ) {
            throw new UnexpectedException(e);
        }
    }

    /**
     * NOTE: Cassandra does not immediately delete the row markers, instead it leaves the keys behind as
     * tombstones until the next grace period where they will be flushed by the index. So the count function
     * needs to be aware that an empty row is the same as no row at all.
     *
     * @param cf Column family containing models for deletion
     */
    public void delete(Class<?> clazz, ColumnFamily<String, String> cf, Object key) {
        try {
            Keyspace keyspace = getKeyspace();
            MutationBatch m = keyspace.prepareMutationBatch();
            m.withRow(cf, key.toString()).delete();
            //m.executeAsync();
            m.execute();
        } catch ( ConnectionException e ) {
            throw new UnexpectedException(e);
        }
    }

    /**
     * NOTE: Cassandra does not immediately delete the row markers, instead it leaves the keys behind as
     * tombstones until the next grace period where they will be flushed by the index. So the count function
     * needs to be aware that an empty row is the same as no row at all.
     *
     * @param cf Column family containing models for deletion
     */
    public void deleteAll(ColumnFamily<String, String> cf) {
        try {
            //Rows<String, String> rows = getAllRows(cf);
            CassandraLogger.info("Deleting column family: %s", cf.getName());
            final Keyspace keyspace = getKeyspace();

            // TruncateColumnFamily too slow - much faster to use Range of indexes with callback using getAllRows special query
            //keyspace.truncateColumnFamily(cf);
            final ColumnFamily<String, String> innerCf = cf;
            final MutationBatch mb = keyspace.prepareMutationBatch();
            keyspace.prepareQuery(cf)
                    .getAllRows()
                    .setRowLimit(100)
                    .setRepeatLastToken(true)
                    .withColumnRange(new RangeBuilder().setLimit(2).build())
                    .executeWithCallback(new RowCallback<String, String>() {
                        @Override
                        public void success(Rows<String, String> rows) {
                            for (Row<String, String> row : rows) {
                                CassandraLogger.debug("DELETING %s[%s]", innerCf.getName(), row.getKey());
                                mb.withRow(innerCf, row.getKey()).delete();
                            }
                        }

                        @Override
                        public boolean failure(ConnectionException e) {
                            return false;
                        }
                    });
            mb.execute();
            // Determine if we have any counters
            ModelReflector reflector = ModelReflector.reflectorFor(cf.getName());
            if ( reflector.hasCounters() ) {
                final ColumnFamily<String, String> counterColumnFamily = reflector.getCounterColumnFamily();
                //keyspace.truncateColumnFamily(counterColumnFamily);
                keyspace.prepareQuery(counterColumnFamily)
                        .getAllRows()
                        .setRowLimit(100)
                        .setRepeatLastToken(true)
                        .withColumnRange(new RangeBuilder().setLimit(25).build())
                        .executeWithCallback(new RowCallback<String, String>() {
                            @Override
                            public void success(Rows<String, String> rows) {
                                for ( Row<String, String>  row : rows ) {
                                    for ( Column<String> column : row.getColumns() ) {
                                        try {
                                            keyspace.prepareColumnMutation(counterColumnFamily, row.getKey(), column.getName())
                                                    .deleteCounterColumn()
                                                    .execute();
                                        } catch (ConnectionException e) {
                                            throw new DatabaseException(e.getMessage());
                                        }
                                    }
                                }
                            }

                            @Override
                            public boolean failure(ConnectionException e) {
                                return false;
                            }
                        });
            }
        } catch ( ConnectionException e ) {
            CassandraLogger.error(e, "Error deleting models");
            throw new UnexpectedException(e);
        }
    }

    /**
     * @param modelClazz Class of type model
     * @param columns Column results from query
     * @param cfName Name of column family
     * @param id Unique object identifier
     * @param <T> Class of type Model
     * @return Populated model from column result
     */
    private <T extends play.db.Model> T parseColumns(Class<? extends play.db.Model> modelClazz, ColumnList<String> columns, String cfName, Object id) {
        try {
            // Detect tombstone (empty column list)
            if ( columns.isEmpty() ) {
                return null;
            }

            T model = (T)modelClazz.newInstance();
            if ( null == model ) {
                return null;
            }

            ModelReflector reflector = ModelReflector.reflectorFor(modelClazz);
            Set<ColumnField> fields = reflector.getColumnFields();
            for (ColumnField field : fields ) {
                if ( field.isCounter() ) {
                    Object value = field.parseValue(getCounterColumn(cfName, id.toString(), field.getName()));
                    if ( null != value ) {
                        field.set(model, value);
                    }
                } else {
                    if ( field.isList() && MapModel.class.isAssignableFrom(field.genericType())) {
                        Class<? extends MapModel> mapClazz = (Class<? extends MapModel>)field.genericType();
                        List<? extends MapModel> mapModels = findComposites(mapClazz, id.toString());
                        field.set(model, mapModels);
                    } else {
                        com.netflix.astyanax.model.Column<String> column = columns.getColumnByName(field.getName());
                        if ( null != column) {
                            getColumn(id.toString(), model, field, columns.getColumnByName(field.getName()));
                        } else if ( field.getName().equals("games")) {
                            throw new UnexpectedException("WHAT GIVES: " + field.getName());
                        }
                    }
                }
            }
            ((Model)model).onLoad();
            return model;
        } catch (InstantiationException e) {
            throw new DatabaseException("Instantiation exception: " + modelClazz.getName(), e);
        } catch (IllegalAccessException e) {
            throw new DatabaseException("Illegal access exception: " + modelClazz.getName(), e);
        } catch (ConnectionException e) {
            throw new DatabaseException("Connection exception: " + modelClazz.getName(), e);
        }
    }

    public List<? extends MapModel> findComposites(Class<? extends MapModel> clazz, String key) {
        String cfName = ModelReflector.reflectorFor(clazz).getColumnFamily().getName();
        return findComposites(cfName, key, clazz);
    }

    public <T extends MapModel> List<T> findComposites(String cfName, String key, Class<? extends MapModel> modelClazz) {
        List<T> models = new ArrayList<T>();
        HashMap<String, T> modelMap = new HashMap<String, T>();
        try
        {
            Keyspace keyspace = getKeyspace();
            ColumnFamily<String, CompositeJoinOrdinals> cf = new ColumnFamily<String, CompositeJoinOrdinals>(cfName,
                    StringSerializer.get(), joinSerializer);

            // Querying cassandra for an entire row
            OperationResult<ColumnList<CompositeJoinOrdinals>> result = keyspace.prepareQuery(cf)
                    .getKey(key)
                    .execute();

            ColumnList<CompositeJoinOrdinals> columns = result.getResult();
            for ( com.netflix.astyanax.model.Column<CompositeJoinOrdinals> column : columns ) {
                CompositeJoinOrdinals join = column.getName();
                T model = modelMap.get(join.dictionaryName);
                if ( null == model ) {
                    model = (T) modelClazz.newInstance();
                    model.setKey1(key);
                    model.setKey2(join.dictionaryName);
                }
                model.put(join.key, column.getStringValue());
                modelMap.put(join.dictionaryName, model);
            }
            for ( T model : modelMap.values()) {
                models.add(model);
            }
        } catch (ConnectionException e) {
            throw new DatabaseException(String.format("Key:%s, %s", key, e.getMessage()), e);
        } catch (InstantiationException e) {
            throw new UnexpectedException("Unable to cast MapModel", e);
        } catch (IllegalAccessException e) {
            throw new UnexpectedException("Unable to cast MapModel", e);
        }
        return models;
    }

    public Map<String, String> findComposite(String cfName, String key, String subkey) {
        HashMap<String, String> map = new HashMap<String, String>();
        try
        {
            Keyspace keyspace = getKeyspace();
            ColumnFamily<String, CompositeJoinOrdinals> cf = new ColumnFamily<String, CompositeJoinOrdinals>(cfName,
                    StringSerializer.get(), joinSerializer);

            // Querying cassandra for an entire row
            OperationResult<ColumnList<CompositeJoinOrdinals>> result = keyspace.prepareQuery(cf)
                    .getKey(key)
                    .execute();

            CassandraLogger.debug("Executing on %s:%s", key, subkey);
            ColumnList<CompositeJoinOrdinals> columns = result.getResult();
            CassandraLogger.debug("Result: %d", columns.size());
            for ( com.netflix.astyanax.model.Column<CompositeJoinOrdinals> column : columns ) {
                CompositeJoinOrdinals join = column.getName();
                // NOTE: Using this instead of getColumnRange because this way lets us return all fields
                if ( join.dictionaryName.equals(subkey)) {
                    CassandraLogger.trace("Parsing a row");
                    map.put(join.key, column.getStringValue());
                }
            }
        } catch (ConnectionException e) {
            throw new DatabaseException(String.format("Key:%s, %s", key, e.getMessage()), e);
        }
        return map;
    }

    public int deleteComposite(String cfName, String key, String subkey, String dictKey) {
        int deletedColumns = 0;
        try
        {
            Keyspace keyspace = getKeyspace();
            ColumnFamily<String, CompositeJoinOrdinals> cf = new ColumnFamily<String, CompositeJoinOrdinals>(cfName,
                    StringSerializer.get(), joinSerializer);

            // Querying cassandra for an entire row
            OperationResult<ColumnList<CompositeJoinOrdinals>> result = keyspace.prepareQuery(cf)
                    .getKey(key)
                    .execute();

            ConsistencyLevel writeConsistencyLevel = config().getDefaultWriteConsistencyLevel();

            CassandraLogger.debug("Executing on %s:%s", key, subkey);
            ColumnList<CompositeJoinOrdinals> columns = result.getResult();
            CassandraLogger.debug("Result: %d", columns.size());
            for ( com.netflix.astyanax.model.Column<CompositeJoinOrdinals> column : columns ) {
                CompositeJoinOrdinals joinOrdinals = column.getName();
                if ( null == dictKey || dictKey.equals(joinOrdinals.key)) {
                    keyspace.prepareColumnMutation(cf, key, joinOrdinals)
                            .setConsistencyLevel(writeConsistencyLevel)
                            .deleteColumn()
                            .execute();
                    deletedColumns++;
                }
            }
        } catch (ConnectionException e) {
            throw new DatabaseException(String.format("Key:%s, %s", key, e.getMessage()), e);
        }
        return deletedColumns;
    }

    // TODO: Perform this function in a single mutation
    public void writeComposite(MapModel model) {
        String cfName = ModelReflector.reflectorFor(model.getClass()).getColumnFamily().getName();
        for ( String key : model.keySet() ) {
            writeComposite(cfName, model.getKey1(), model.getKey2(), key, model.get(key));
        }
    }

    /**
     *
     * @param cfName
     * @param rowKey User Id
     * @param dictionaryKey Game Id
     * @param key Goal Name
     * @param value Goal Value
     */
    public void writeComposite(String cfName, String rowKey, String dictionaryKey, String key, String value ) {
        // Default no expiry
        writeComposite(cfName, rowKey, dictionaryKey, key, value, null);
    }

    public void writeComposite(String cfName, String rowKey, String dictionaryKey, String key, String value, Integer ttl ) {
        try
        {
            CassandraLogger.debug("Writing a composite column: %s: %s, %s, %s, %s", cfName, rowKey, dictionaryKey, key, value);
            Keyspace keyspace = getKeyspace();
            ColumnFamily<String, CompositeJoinOrdinals> cf = new ColumnFamily<String, CompositeJoinOrdinals>(cfName,
                    StringSerializer.get(), joinSerializer);
            CompositeJoinOrdinals joinOrdinals = new CompositeJoinOrdinals();
            joinOrdinals.dictionaryName = dictionaryKey;
            joinOrdinals.key = key;

            keyspace.prepareColumnMutation(cf, rowKey, joinOrdinals)
                    .putValue(value, ttl)
                    .execute();
        } catch (ConnectionException e) {
            throw new DatabaseException(String.format("Key:%s, %s", rowKey, e.getMessage()), e);
        }
    }

    public Model find(Class<? extends play.db.Model> clazz, ColumnFamily<String, String> cf, Object id) {
        Model model = null;
        try
        {
            Keyspace keyspace = getKeyspace();

            OperationResult<ColumnList<String>> result =
                    keyspace.prepareQuery(cf)
                            .getKey(id.toString())
                            .execute();
            ColumnList<String> columns = result.getResult();

            if ( !columns.isEmpty() ) {
                CassandraLogger.debug("Found row seeking ID %s", id.toString());
                model = parseColumns(clazz, columns, cf.getName(), id);
            }
        } catch (ConnectionException e) {
            throw new DatabaseException(String.format("Key:%s, %s", id.toString(), e.getMessage()), e);
        }
        return model;
    }

    public List<? extends Model> findByKey(Class<? extends play.db.Model> clazz, ColumnFamily<String, String> cf, String key, Object value) {
        ModelReflector mf = ModelReflector.reflectorFor(clazz);
        List<Model> models = new ArrayList<Model>();
        try
        {
            Keyspace keyspace = getKeyspace();

            OperationResult<Rows<String, String>> result =
                    keyspace.prepareQuery(cf)
                            .searchWithIndex()
                            .addExpression()
                                .whereColumn(key).equals().value(value.toString())
                            .execute();

            Rows<String,String> rows = result.getResult();
            for ( Row<String,String> row : rows ) {
                ColumnList<String> columns = row.getColumns();
                String id = columns.getColumnByName(mf.getIdentityField()).getStringValue();

                if ( !columns.isEmpty() ) {
                    CassandraLogger.debug("Found row seeking Key %s with value %s: ID - %s", key, value.toString(), id);
                    Model model = parseColumns(clazz, columns, cf.getName(), id);
                    models.add(model);
                }
            }
        } catch (ConnectionException e) {
            throw new DatabaseException(String.format("Key:%s, %s", key, value.toString()), e);
        }
        return models;
    }

    /**
     * Why do this instead of use Astyanax' Mapper class?
     * Because we can't use PUT and SET with extend in Java, so
     * we can't go down the route of generics... so we build
     * a HashMap instead
     *
     * @param o Reference to model being saved into the data store
     * @param cf Column family the model belongs to
     */
    public void save(Model o, ColumnFamily<String, String> cf) {
        save(o, cf, false);
    }
    /**
     * Why do this instead of use Astyanax' Mapper class?
     * Because we can't use PUT and SET with extend in Java, so
     * we can't go down the route of generics... so we build
     * a HashMap instead
     *
     * @param o Model instance with fields to be saved
     * @param cf Column family model belongs to
     * @param saveCounters TRUE if counters should be explicitly set during this save
     */
    public void save(Model o, ColumnFamily<String, String> cf, boolean saveCounters) {
        try {
            ConsistencyLevel writeConsistencyLevel = config().getDefaultWriteConsistencyLevel();
            CassandraLogger.trace("Saving a cassandra model");

            if ( null == o) {
                throw new UnexpectedException("Model is null");
            }

            if ( null == o.getId() ) {
                throw new UnexpectedException("ID is null");
            }

            if ( null == cf ) {
                throw new UnexpectedException("ColumnFamily is null");
            }

            if ( o.isNew() ) {
                CassandraLogger.debug(String.format("Saving new model %s[%s]", cf.getName(), o.getId().toString()));
                writeConsistencyLevel = ConsistencyLevel.valueOf(Play.configuration.getProperty("cassandra.concurrency.write.model", "CL_QUORUM"));
            } else {
                CassandraLogger.debug(String.format("Saving updated model %s[%s]", cf.getName(), o.getId().toString()));
            }

            Keyspace keyspace = getKeyspace();
            MutationBatch mutationBatch = keyspace.prepareMutationBatch();
            mutationBatch.setConsistencyLevel(writeConsistencyLevel);
            ColumnListMutation<String> columnListMutation = mutationBatch.withRow(cf, o.getId().toString());

            // Inserting data
            ModelReflector reflector = ModelReflector.reflectorFor(cf.getName());
            Set<ColumnField> fields = reflector.getColumnFields();
            for (ColumnField field : fields ) {
                String columnName = field.getName();
                if ( field.isCounter() ) {
                    if ( saveCounters ) {
                        debug("Performing putCounter on %s", columnName);
                        putCounterColumn(o, cf.getName(), o.getId().toString(), columnName, field);
                    }
                } else {
                    putColumn(o, columnListMutation, columnName, field);
                }
            }

            if ( o.isNew() ) {
                mutationBatch.execute();
            } else {
                //mutationBatch.executeAsync();
                mutationBatch.execute();
            }
        } catch (Exception e) {
            CassandraLogger.error(e, "Could not save a Cassandra object");
            throw new UnexpectedException(e);
        }
    }

    /**
     * NOTE: Cassandra does not immediately delete the row markers, instead it leaves the keys behind as
     * tombstones until the next grace period where they will be flushed by the index. So the count function
     * needs to be aware that an empty row is the same as no row at all.
     *
     * @param cf Column family to count
     * @return long Number of models in family
     */
    public long count(ColumnFamily<String, String> cf) {
        try {
            Keyspace keyspace = getKeyspace();

            OperationResult<Rows<String, String>> results = keyspace.prepareQuery(cf)
                    .getAllRows()
                    .execute();

            Rows<String, String> rows = results.getResult();

            long result = 0;
            //noinspection UnusedDeclaration,UnusedDeclaration
            for ( Row<String, String> row : rows ) {
                if ( ! row.getColumns().isEmpty() ) {
                    result++;
                }
            }

            return result;
        } catch ( ConnectionException e ) {
            CassandraLogger.error(e, "Could not count a Cassandra object");
            throw new UnexpectedException(e);
        }
    }

    /**
	 * WOAH - DO NOT BUILD THE COLUMN FAMILY HERE - ITS DONE IN GETCOUNTERCOULUMN TOO
	 * @param id Key value for row
	 * @param cfName Name of row's keyfamily
	 * @param columnName Name of column being incremented
	 * @param amount Amount by which column is increment
	 * @return TRUE when incrementation is successful
	 */
	@SuppressWarnings("SameReturnValue")
    public boolean increment(String id, String cfName, String columnName, long amount) {
        try {
            Keyspace keyspace = getKeyspace();

            keyspace.prepareColumnMutation(ModelReflector.reflectorFor(cfName).getCounterColumnFamily(), id, columnName)
                .incrementCounterColumn(amount)
                //.executeAsync();
                .execute();


            return true;
        } catch ( ConnectionException e ) {
            CassandraLogger.error(e, "Connection exception trying to increment a Cassandra object: " + cfName);
            throw new UnexpectedException(e);
        }
	}

	/**
	 * Passes values in as close to primitive types as possible. Other types (such as BigDecimal, UUI) which may
	 * require special processing to read are passed as strings
	 *
	 * @param o Reference to model being updated
	 * @param columnListMutation Reference to affected query
	 * @param columnName Name of column being saved
	 * @param modelField Field property from model - this is being inspected for type, value and name
	 */
	private void putColumn(Model o, ColumnListMutation<String> columnListMutation, String columnName, ColumnField modelField) {
		try {
            if ( null == modelField.get(o)) {
                columnListMutation.putEmptyColumn(columnName, null);
                return;
            }
			Class<?> objClazz = modelField.getType();
			if ( play.db.Model.class.isAssignableFrom(objClazz) ) {
				play.db.Model model = (play.db.Model)modelField.get(o);
                columnListMutation.putColumn(columnName, model._key().toString(), null);
            } else if ( modelField.isList() ) {
                if ( play.db.Model.class.isAssignableFrom(modelField.genericType())) {
                    List<String> idList = new ArrayList<String>();
                    List<play.db.Model> relations = (List<play.db.Model>)modelField.get(o);
                    if ( null != relations ) {
                        for ( play.db.Model model : relations ) {
                            if ( null != model && null != model._key() ) {
                                idList.add(model._key().toString());
                            }
                        }
                    }
                    columnListMutation.putColumn(columnName, new Gson().toJson(idList), null);
                } else if ( MapModel.class.isAssignableFrom(modelField.genericType())) {
                    List<MapModel> mapModels = (List<MapModel>)modelField.get(o);
                    for ( MapModel mapModel : mapModels ) {
                        if ( mapModel.isChanged() ) {
                            writeComposite(mapModel);
                        }
                    }
                }
            } else if ( Date.class.isAssignableFrom(objClazz) ) {
				columnListMutation.putColumn(columnName, (Date)modelField.get(o), null);
			} else if ( objClazz.isAssignableFrom(Boolean.TYPE)) {
				columnListMutation.putColumn(columnName, modelField.getBoolean(o), null);
            } else if ( objClazz.isAssignableFrom(Boolean.class)) {
                columnListMutation.putColumn(columnName, (Boolean)modelField.get(o), null);
            } else if ( objClazz.isAssignableFrom(Double.TYPE)) {
				columnListMutation.putColumn(columnName, modelField.getDouble(o), null);
            } else if ( objClazz.isAssignableFrom(Double.class)) {
                columnListMutation.putColumn(columnName, (Double)modelField.get(o), null);
            } else if ( objClazz.isAssignableFrom(Integer.TYPE)) {
				columnListMutation.putColumn(columnName, modelField.getInt(o), null);
            } else if ( objClazz.isAssignableFrom(Integer.class)) {
                columnListMutation.putColumn(columnName, (Integer)modelField.get(o), null);
            } else if ( objClazz.isAssignableFrom(Long.TYPE)) {
				columnListMutation.putColumn(columnName, modelField.getLong(o), null);
            } else if ( objClazz.isAssignableFrom(Long.class)) {
                columnListMutation.putColumn(columnName, (Long)modelField.get(o), null);
            } else {
				Object value = modelField.get(o);
				if ( null == value ) {
					columnListMutation.putEmptyColumn(columnName, null);
				} else {
					columnListMutation.putColumn(columnName, value.toString(), null);
				}
			}
		} catch ( NullPointerException e) {
			columnListMutation.putEmptyColumn(columnName, null);
		} catch ( IllegalAccessException e ) {
			throw new UnexpectedException(e);
		}
	}

	@SuppressWarnings("unchecked")
	private void getColumn(String rowKey, Object o, ColumnField field, com.netflix.astyanax.model.Column<String> column) throws IllegalArgumentException, IllegalAccessException {
        Class<?> objClazz = field.getType();
        try {
			String value = column.getStringValue();
			if ( play.db.Model.class.isAssignableFrom(objClazz) ) {
				Object relationValue = play.db.Model.Manager.factoryFor(ModelReflector.reflectorFor(objClazz).getType()).findById(value);
				field.set(o, relationValue);
            } else if ( Collection.class.isAssignableFrom(objClazz) ) {
                RelationList list = new RelationList();
                String [] ids = new Gson().fromJson(value, String[].class);
                for ( String id : ids ) {
                    list.add(new Relation(field.genericType().getName(), id));
                }
                field.set(o, list);
            } else if ( Date.class.isAssignableFrom(objClazz) ) {
				Date dateValue = column.getDateValue();
				field.set(o, dateValue);
			} else if ( BigDecimal.class.isAssignableFrom(objClazz) ) {
				if ( null != value ) {
					try {
						field.set(o, new BigDecimal(value));
					} catch ( NumberFormatException e) {
						// Let it slide, Model constructor generates default value
					}
				}
			} else if ( objClazz.isAssignableFrom(Boolean.TYPE)  ||  objClazz.isAssignableFrom(Boolean.class)) {
				boolean iValue = column.getBooleanValue();
				field.set(o, iValue);
			} else if ( objClazz.isAssignableFrom(Double.TYPE)  ||  objClazz.isAssignableFrom(Double.class)) {
				double iValue = column.getDoubleValue();
				field.set(o, iValue);
			} else if ( objClazz.isAssignableFrom(Integer.TYPE) ||  objClazz.isAssignableFrom(Integer.class)) {
				int iValue = column.getIntegerValue();
				field.set(o, iValue);
			} else if ( objClazz.isAssignableFrom(Long.TYPE) ||  objClazz.isAssignableFrom(Long.class)) {
				long dValue = column.getLongValue();
				field.set(o, dValue);
			} else if ( objClazz.isAssignableFrom(UUID.class)) {
				UUID uuidValue = column.getUUIDValue();
				field.set(o, uuidValue);
			} else if ( objClazz.isAssignableFrom(ByteBuffer.class)) {
				ByteBuffer bbValue = column.getByteBufferValue();
				field.set(o, bbValue);
            }
            else if ( objClazz.isAssignableFrom(DateTime.class)) {
                DateTime dtValue = null;
                if ( !value.isEmpty() ) {
                    dtValue = DateTime.parse(value);
                }
                field.set(o, dtValue);
            } else if ( objClazz.isEnum() ) {
                if ( null != value && !value.toString().isEmpty() ) {
                    try {
                        field.set(o, Enum.valueOf((Class<Enum>) field.getType(), value));
                    } catch ( IllegalArgumentException e ) {
                        // do nothing - allow fallback to default value
                    }
                }
			} else {
				field.set(o, value);
			}
		} catch (IllegalAccessException e) {
			throw new DatabaseException("Unable to read column - IllegalAccessException");
		} catch (NullPointerException e) {
            if ( field.getName().equals("games")) throw new UnexpectedException(e);
            // We just don't care
		}
	}

    private void deleteCounters(String cfName, String rowKey, String[] counterColumns) {
        ColumnFamily<String, String> cf = ModelReflector.reflectorFor(cfName).getCounterColumnFamily();
        Keyspace keyspace = getKeyspace();
        // Why not a mutation batch? Because deleteCounterColumn() doesn't work that way
        for ( String columnName : counterColumns ) {
            try {
                keyspace.prepareColumnMutation(cf, rowKey, columnName)
                        .deleteCounterColumn()
                        //.executeAsync();
                        .execute();
            } catch (ConnectionException e) {
                throw new UnexpectedException(e);
            }
        }
    }

    /**
     * TODO: Analyze the flow of this - should we keep retrying until the results are as expected; can this create a dangerous recursive deadlock situation?
     * TODO: Not DRY - getCounterColumn repeats part of this functionality
     * @param o Reference to object sending the counter
     * @param cfName Name of column family
     * @param rowKey Unique identifier of object(o) in column family
     * @param columnName Name of column being changed
     * @param modelField Reference to class field which the operation is being done on. TODO: This is silly, we should be passing in the value here
     * @throws IllegalAccessException Occurs when a column is accessed inappropriately (for example, attempting to save to a null)
     */
    private void putCounterColumn(Object o, String cfName, String rowKey, String columnName, ColumnField modelField) throws IllegalAccessException, ConnectionException {
        putCounterColumn(cfName, rowKey, columnName, modelField, modelField.get(o), 1);
    }

    private void putCounterColumn(String cfName, String rowKey, String columnName, ColumnField modelField, Object value, Integer attempt) throws IllegalAccessException, ConnectionException {
        Long counterValue;
        try {
            counterValue = getCounterColumn(cfName, rowKey, columnName, ConsistencyLevel.valueOf(Play.configuration.getProperty("cassandra.concurrency.read.counter", "CL_QUORUM")));
        } catch (NotFoundException e)  {
            counterValue = 0L;
        }

        if ( null == counterValue ) {
            counterValue = 0L;
        }

        Long targetValue = modelField.toLong(value);

        if ( null == targetValue ) {
            targetValue = 0L;
        }

        if ( !counterValue.equals(targetValue) ) {
            ColumnFamily<String, String> cf = ModelReflector.reflectorFor(cfName).getCounterColumnFamily();
            Keyspace keyspace = getKeyspace();
            debug("Column %s is set to %d, but expected %d", columnName, counterValue, targetValue);
            Long diff = targetValue - counterValue;
            debug("Column %s incrementing by %d", columnName, diff);

            keyspace.prepareColumnMutation(cf, rowKey, columnName)
                    .incrementCounterColumn(diff)
                    .execute();

            // Call self - goal is to read the value, ensure it matches expectations,
            // and re-increment if another change has been persisted since our update
            if ( attempt > 3 ) {
                throw new UnexpectedException(String.format("Giving up on putCounterColumn after %s attempts attempting to save %d into %s on %s (found value: %d)", attempt, targetValue, columnName, cfName, counterValue));
            }
            putCounterColumn(cfName, rowKey, columnName, modelField, value, attempt+1);
        }
    }

    /**
	 * @param cfName Name of column family being searched
	 * @param rowKey ID of row in column family
     * @param columnName Name of column being queried
     * @throws com.netflix.astyanax.connectionpool.exceptions.ConnectionException Thrift Client throws connection error if any kind of database-related exception occurs
     * @return Value from counter (must always be Long)
	 */
    private Long getCounterColumn(String cfName, String rowKey, String columnName) throws ConnectionException {
        return getCounterColumn(cfName, rowKey, columnName, config().getDefaultWriteConsistencyLevel());
    }

    private Long getCounterColumn(String cfName, String rowKey, String columnName, ConsistencyLevel consistencyLevel) throws ConnectionException {
        Long value;

        Keyspace keyspace = getKeyspace();

        try {
            com.netflix.astyanax.model.Column<String> result = keyspace.prepareQuery(ModelReflector.reflectorFor(cfName).getCounterColumnFamily())
                    .setConsistencyLevel(consistencyLevel)
                    .getKey(rowKey)
                    .getColumn(columnName)
                    .execute()
                    .getResult();

            value = result.getLongValue();
        } catch ( NotFoundException e ) {
            value = null;
        }
        return value;
	}

    private Keyspace getKeyspace() {
        return _context.getEntity();
    }

	public AstyanaxContext<Keyspace> getContext() {
		return _context;
	}

    public <T> T getColumnValue(String cfName, String rowKey, String columnName) {
        try {
            T value;

            Keyspace keyspace = getKeyspace();

            final ColumnFamily<String, String> CF_COUNTER = ModelReflector.reflectorFor(cfName).getCounterColumnFamily();

            try {
                com.netflix.astyanax.model.Column<String> result = keyspace.prepareQuery(CF_COUNTER)
                        .getKey(rowKey)
                        .getColumn(columnName)
                        .execute()
                        .getResult();
                value = (T)new Long(result.getStringValue());
            } catch ( NotFoundException e ) {
                value = null;
            }
            return value;
        } catch ( Exception e ) {
            throw new UnexpectedException(e);
        }
    }

    public void setColumnValue(String cfName, String rowKey, String columnName, Object value) {
        try {
            ModelReflector reflector = ModelReflector.reflectorFor(cfName);
            ColumnField columnField = reflector.getColumnField(columnName);
            if ( null == columnField ) {
                throw new UnexpectedException(String.format("%s: Could not find column field %s", cfName, columnName));
            }

            if ( columnField.isCounter()) {
                putCounterColumn(cfName, rowKey, columnName, columnField, value, 1);
            } else {
                ColumnFamily<String, String> cf = reflector.getColumnFamily();
                Keyspace keyspace = getKeyspace();

                keyspace.prepareColumnMutation(cf, rowKey, columnName)
                        .putValue(value.toString(), null)
                        //.executeAsync();
                        .execute();
            }
        } catch ( Exception e ) {
            throw new DatabaseException(e.getMessage());
        }
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
        StringBuilder sb = new StringBuilder();
        List<? extends HostConnectionPool<?>> pools = _context.getConnectionPool().getActivePools();
        for ( HostConnectionPool pool : pools ) {
            sb.append(String.format("  %s\r\n", pool.getHost().getHostName()));
        }
        return  sb.toString();
    }
}
