package play.modules.cassandra;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.mapping.Id;
import com.netflix.astyanax.model.ConsistencyLevel;
import play.Play;
import play.PlayPlugin;
import play.classloading.ApplicationClasses.ApplicationClass;
import play.db.Model;
import play.db.Model.Factory;
import play.exceptions.UnexpectedException;
import play.modules.cassandra.jetlang.CassandraMonitor;
import play.modules.cassandra.providers.CassandraProvider;
import play.modules.cassandra.providers.H2Provider;

import javax.persistence.ManyToMany;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.math.BigDecimal;
import java.util.*;

public class CassandraPlugin extends PlayPlugin {
	public static final String VERSION = "0.5.6";
	private final CassandraEnhancer e_ = new CassandraEnhancer();
    private static CassandraDB _instance;
    private static CassandraMonitor _cassandraMonitor;

    private static CassandraDB initDataStore(Boolean useMemDb) {
        if ( null == _instance ) {
            if ( useMemDb ) {
                _instance = H2Provider.instance();
            } else {

                _instance = CassandraProvider.instance();
            }
        }
        return _instance;
    }

    public static Keyspace getRawKeyspace() {
        return _instance.getRawKeyspace();
    }

    public static void deleteMapModels() {
        List<Class> entities = Play.classloader.getAnnotatedClasses(play.modules.cassandra.annotations.Entity.class);
        for ( Class entity : entities ) {
            if ( MapModel.class.isAssignableFrom(entity)) {
                _instance.deleteAll(ModelReflector.reflectorFor(entity.getSimpleName()).getColumnFamily());
            }
        }
    }

    @Override
    public String getStatus() {
        ConsistencyLevel defaultWriteConsistency = ConsistencyLevel.valueOf(Play.configuration.getProperty("cassandra.concurrency.write.default", "CL_QUORUM"));
        ConsistencyLevel defaultReadConsistency = ConsistencyLevel.valueOf(Play.configuration.getProperty("cassandra.concurrency.read.default", "CL_QUORUM"));
        NodeDiscoveryType nodeDiscoveryType = NodeDiscoveryType.valueOf(Play.configuration.getProperty("cassandra.nodediscoverytype", "NONE"));
        ConnectionPoolType connectionPoolType = ConnectionPoolType.valueOf(Play.configuration.getProperty("cassandra.connectionpooltype", "BAG"));


        StringBuilder sb = new StringBuilder();
        sb.append("Cassandra Datasource\r\n");
        sb.append("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\r\n");
        sb.append(String.format("Provider Type:             %s\r\n", _instance.getProviderType()));
        sb.append(String.format("Default Write Consistency: %s\r\n", defaultWriteConsistency.toString()));
        sb.append(String.format("Default Read Consistency:  %s\r\n", defaultReadConsistency.toString()));
        sb.append(String.format("Node Discovery Type:       %s\r\n", nodeDiscoveryType.toString()));
        sb.append(String.format("Connection Pool Type:      %s\r\n", connectionPoolType.toString()));
        sb.append("Hosts:\r\n" + _instance.getHosts());

        return sb.toString();
    }

    @Override
	public void onApplicationStart() {
        _cassandraMonitor = new CassandraMonitor(_instance);
        Boolean useMemDb = Boolean.parseBoolean(Play.configuration.getProperty("cassandra.usememdb", "false"));
        initDataStore(useMemDb);
    }

	@Override
	public void onApplicationStop() {
        _cassandraMonitor.dispose();
	}
	
	@Override
	public void enhance(ApplicationClass applicationClass) throws Exception {
		e_.enhanceThisClass(applicationClass);
	}
    
    public static CassandraDB ds() {
    	return initDataStore(false);
    }

    @Override
    @SuppressWarnings({"unchecked", "deprecation"})
    public Object bind(String name, @SuppressWarnings("rawtypes") Class clazz,
                       java.lang.reflect.Type type, Annotation[] annotations,
                       Map<String, String[]> params) {
        if (Model.class.isAssignableFrom(clazz)) {
            String keyName = modelFactory(clazz).keyName();
            String idKey = name + "." + keyName;
            if (params.containsKey(idKey) && params.get(idKey).length > 0
                    && params.get(idKey)[0] != null
                    && params.get(idKey)[0].trim().length() > 0) {
                String id = params.get(idKey)[0];
                try {
                    ModelReflector reflector = ModelReflector.reflectorFor(clazz);
                    Object o = ds().find(clazz, reflector.getColumnFamily(), id );
                    if ( null == o) {
                        return play.modules.cassandra.Model.create(clazz, name, params, annotations);
                    }
                    return play.modules.cassandra.Model.edit(o, name, params, annotations);
                } catch (Exception e) {
                    throw new UnexpectedException(e);
                }
            }
            return play.modules.cassandra.Model.create(clazz, name, params, annotations);
        }
        return super.bind(name, clazz, type, annotations, params);
    }

    @SuppressWarnings("deprecation")
    @Override
    public Object bind(String name, Object o, Map<String, String[]> params) {
        if (o instanceof Model) {
            return play.modules.cassandra.Model.edit(o, name, params, null);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Model.Factory modelFactory(Class<? extends play.db.Model> modelClass) {
        if (Model.class.isAssignableFrom(modelClass)
                && modelClass.isAnnotationPresent(play.modules.cassandra.annotations.Entity.class)) {
            return CassandraModelLoader
                    .getFactory((Class<play.modules.cassandra.Model>)modelClass);
        }
        return null;
    }
	
    public static class CassandraModelLoader implements Model.Factory {
    	private static final Map<Class<? extends play.db.Model>, Model.Factory> m_ = new HashMap<Class<? extends Model>, Factory>();
    	
    	private final Class<? extends play.modules.cassandra.Model> clazz;
        private final ModelReflector reflector;

    	public CassandraModelLoader(Class<? extends play.modules.cassandra.Model> clazz) {
			this.clazz = clazz;
            this.reflector = ModelReflector.reflectorFor(clazz);
			m_.put(clazz, this);
		}

		public static Model.Factory getFactory(Class<? extends play.modules.cassandra.Model> clazz) {
    		synchronized (m_) {
    			Model.Factory f = m_.get(clazz);
    			if ( null == f) {
    				f = new CassandraModelLoader(clazz);
    			}
				return f;
    		}
    	}
		
		public void save(play.modules.cassandra.Model o, boolean saveCounters) {
            //if ( Play.mode == Play.Mode.DEV) {
                ds().save(o, reflector.getColumnFamily(), saveCounters);
            //} else {
            //    _cassandraMonitor.publishSave(o, saveCounters);
            //}
		}
		
		@Override
		public String keyName() {
			return keyField().getName();
		}

		@Override
		public Class<?> keyType() {
			return String.class;
		}

		@Override
		public Object keyValue(Model m) {
            Field k = keyField();
            try {
                // Embedded class has no key value
                Object value = (null != k ? k.get(m) : null);
                return null != k ? value : null;
            } catch (Exception ex) {
                throw new UnexpectedException(ex);
            }
        }
        
        Field keyField() {
            Class<?> c = clazz;
            try {
                while (!c.equals(Object.class)) {
                    for (Field field : c.getDeclaredFields()) {
                        if (field.isAnnotationPresent(Id.class)) {
                            field.setAccessible(true);
                            return field;
                        }
                    }
                    c = c.getSuperclass();
                }
            } catch (Exception e) {
                throw new UnexpectedException(
                        "Error while determining the object @Id for an object of type "
                                + c);
            }
            return null;
        }

        public <T> T getColumnValue(String id, String column) {
            ColumnField columnField = reflector.getColumnField(column);
            if ( null == columnField ) {
                throw new UnexpectedException("Column type is not found");
            }
            return ds().getColumnValue(reflector.getColumnFamily().getName(), id, columnField.getName());
        }

        public void setColumnValue(String id, String column, Object value) {
            ColumnField columnField = reflector.getColumnField(column);
            if ( null == columnField ) {
                throw new UnexpectedException("Column type is not found");
            }

            ds().setColumnValue(reflector.getColumnFamily().getName(), id, columnField.getName(), value);
        }

        public boolean increment(String id, String column, BigDecimal amount) {
            ColumnField dbColumn = reflector.getColumnField(column);
            if ( null == dbColumn ) {
                return false;
            }
            Long changeAmount;
            if ( !dbColumn.isCounter()) {
                throw new UnexpectedException("BigDecimal column is not a counter type");
            }

            changeAmount = dbColumn.toLong(amount);
            return ds().increment(id, reflector.getColumnFamily().getName(), dbColumn.getName(), changeAmount);
        }

        public boolean increment(String id, String column, long amount) {
            ColumnField dbColumn = reflector.getColumnField(column);
            if ( null == dbColumn ) {
                return false;
            }
            if ( !dbColumn.isCounter()) {
                throw new UnexpectedException("This column is not a counter type");
            }
            return ds().increment(id, reflector.getColumnFamily().getName(), dbColumn.getName(), amount);
        }

        @Override
		public Model findById(Object id) {
            try {
                if ( null == id || id.toString().trim().isEmpty() ) {
                    return null;
                }

                Model model = ds().find(clazz, reflector.getColumnFamily(), id);
                if ( null == model ) {
                    return null;
                }
                Field keyfield = keyField();
                if ( null != keyfield) {
                        keyfield.set(model, id);
                }
                return model;
            } catch (IllegalAccessException e) {
                throw new UnexpectedException(e);
            }
		}

        @SuppressWarnings("unchecked")
        public List<? extends Model> findByKey(String key, Object value) {
            try {
                ColumnField field = reflector.getColumnField(key);
                if ( !field.isIndex() ) {
                    throw new UnexpectedException("Key " + key + " is not an index");
                }
                List<? extends Model> models = ds().findByKey(clazz, reflector.getColumnFamily(), field.getName(), value);
                return models;
            } catch (Exception e) {
                throw new UnexpectedException(e);
            }
        }

		@Override
		public List<Model> fetch(int offset, int length, String orderBy,
				String orderDirection, List<String> properties,
				String keywords, String where) {
			return null;
		}

		@Override
		public Long count(List<String> properties, String keywords, String where) {
            return null;
		}

        public long count() {
            try {
                return ds().count(reflector.getColumnFamily());
            } catch ( Exception e) {
                throw new UnexpectedException(e);
            }
        }

        @Override
        public void deleteAll() {
            //if ( Play.mode == Play.Mode.DEV ) {
                ds().deleteAll(reflector.getColumnFamily());
            //} else {
            //    _cassandraMonitor.publishDelete(reflector.getColumnFamily());
            //}
        }

        public void delete(play.modules.cassandra.Model model) {
            ds().delete(clazz, reflector.getColumnFamily(), model._key());
        }

        public CassandraQuery findAll() {
            return new CassandraQuery(ds().all(clazz));
        }

        @Override
		public List<Model.Property> listProperties() {
			List<Model.Property> properties = new ArrayList<Model.Property>();
			Set<Field> fields = new HashSet<Field>();
			Class<?> tclazz = clazz;
			while (!tclazz.equals(Object.class)) {
				Collections.addAll(fields, tclazz.getDeclaredFields());
				tclazz = tclazz.getSuperclass();
			}
            for (Field f : fields) {
                if (Modifier.isTransient(f.getModifiers())) {
                    continue;
                }
                if (Modifier.isStatic(f.getModifiers())) {
                    continue;
                }
                Model.Property mp = buildProperty(f);
                if (mp != null) {
                    properties.add(mp);
                }
            }
            return properties;
		}
		
        Model.Property buildProperty(final Field field) {
            Model.Property modelProperty = new Model.Property();
            modelProperty.type = field.getType();
            modelProperty.field = field;
            if (Model.class.isAssignableFrom(field.getType())) {
                modelProperty.isRelation = true;
                modelProperty.relationType = field.getType();
            }
            if ( Collection.class.isAssignableFrom(field.getType())) {
                final Class<?> fieldType = (Class<?>) ((ParameterizedType)field.getGenericType()).getActualTypeArguments()[0];
                if ( field.isAnnotationPresent(ManyToMany.class)) {
                    if (field.getAnnotation(ManyToMany.class).mappedBy().equals("")) {
                        modelProperty.isRelation = true;
                        modelProperty.isMultiple = true;
                        modelProperty.relationType = fieldType;
                        modelProperty.choices = new Model.Choices() {
                            @SuppressWarnings("unchecked")
                            public List<Object> list() {
                                return (List<Object>) findAll().fetch();
                            }
                        };
                    }
                }
            }
            if ( field.getType().isEnum()) {
                modelProperty.choices = new Model.Choices() {
                    @SuppressWarnings("unchecked")
                    public List<Object> list() {
                        return Arrays.asList(field.getType().getEnumConstants());
                    }
                };
            }
            modelProperty.name = field.getName();
            if (field.getType().equals(String.class)) {
                modelProperty.isSearchable = true;
            }
            return modelProperty;
        }
    }
}
