package play.modules.cassandra;

import org.joda.time.DateTime;
import play.Play;
import play.data.binding.BeanWrapper;
import play.data.validation.Validation;
import play.exceptions.UnexpectedException;

import javax.persistence.ManyToMany;
import javax.persistence.Transient;
import java.io.Serializable;
import java.lang.annotation.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.*;

public class Model implements Serializable, play.db.Model
{
    /** Automatic timestamping **/
    public Long created;
    public Long updated;

	private static final long serialVersionUID = -7158363129909798630L;
	
	public static play.db.Model.Factory getModelFactory() {
        throw new UnsupportedOperationException(
                "Please annotate your model with @play.modules.cassandra.annotations.Entity annotation.");
    }

    // -- helper utilities
    @Transient
    private transient boolean saved_ = false;

    /**
     * A utility method determine whether this entity is a newly constructed
     * object in memory or represents a data from Cassandra
     *
     * @return true if this is a memory object which has not been saved to db
     *         yet, false otherwise
     */
    public final boolean isNew() {
        return !saved_;
    }

    // Event triggers when model is loaded from ORM
    public void onLoad() {
        setSaved_();
    }

    private void setSaved_() {
        saved_ = true;
    }

    @Override
	public void _save() {
		save(isNew());
	}

    public Object getId() {
        throw new UnsupportedOperationException("getId: Please annotate your model with @com.netflix.astyanax.mapping.Id annotation.");
    }

	@Override
	public void _delete() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object _key() {
		return getId();
	}

    public <T extends play.db.Model> T save() {
        return save(isNew());
    }

    @SuppressWarnings("unchecked")
    public <T extends play.db.Model> T save(boolean saveCounters) {
        CassandraPlugin.CassandraModelLoader cmf = (CassandraPlugin.CassandraModelLoader)CassandraPlugin.CassandraModelLoader.getFactory(getClass());
        if ( isNew() ) created = DateTime.now().getMillis();
        updated = DateTime.now().getMillis();
        cmf.save(this, saveCounters);
        if ( isNew()) setSaved_();
        return (T) this;
    }

    // -- porting from play.db.GenericModel
    @SuppressWarnings("unchecked")
    public static <T extends Model> T create(Class<T> type, String name,
            Map<String, String[]> params, Annotation[] annotations) {
        try {
            Constructor<T> c = type.getDeclaredConstructor();
            c.setAccessible(true);
            Object model = c.newInstance();
            return (T) edit(model, name, params, annotations);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends Model> T edit(Object o, String name,
            Map<String, String[]> params, Annotation[] annotations) {
        try
    	{
            BeanWrapper bw = new BeanWrapper(o.getClass());
	    	Set<Field> fields = new HashSet<Field>();
	    	Class<?> clazz = o.getClass();
	    	while (!clazz.equals(Object.class)) {
	    		Collections.addAll(fields, clazz.getDeclaredFields());
	    		clazz = clazz.getSuperclass();
	    	}
	    	for ( Field field : fields ) {
                boolean isEntity = false;
                boolean multiple = false;
                String relation = null;
                int setter = 0;

	    		Class<?> clz = field.getType();
	    		if ( play.db.Model.class.isAssignableFrom(clz)) {
	    			isEntity = true;
                    relation = clz.getName();
                }
                if ( field.isAnnotationPresent(ManyToMany.class)) {
                    isEntity = true;
                    multiple = true;

                    relation = ((Class<?>) ((ParameterizedType) field
                        .getGenericType()).getActualTypeArguments()[0]).getName();
                }

                if ( isEntity ) {
                    String paramKey = name + "." + field.getName();
                    Class<Model> c = (Class<Model>) Play.classloader
                            .loadClass(relation);
                    String keyName = Model.Manager.factoryFor(c).keyName();
                    paramKey = paramKey + "." + keyName;

                    if ( multiple && Collection.class.isAssignableFrom(field.getType())) {
                        RelationList<Model> l = new RelationList<Model>();

                        CassandraLogger.debug("Collection initialized: %1$s", l.getClass().getName());

                        String[] ids = params.get(paramKey);
                        if ( ids != null ) {
                            params.remove(paramKey);
                            for ( String id : ids ) {
                                l.add(new Relation<Model>(relation, id));
                            }
                        }
                        bw.set(field.getName(), o, l);
                    } else {
                        Class<T> vClazz = (Class<T>)Play.classloader.loadClass(relation);
                        String[] ids = params.get(paramKey);
                        if ( ids != null && ids.length > 0 && !ids[0].equals("")) {
                            params.remove(paramKey);
                            try {
                                play.db.Model model = play.db.Model.Manager.factoryFor(vClazz).findById(ids[0]);
                                bw.set(field.getName(), o, model);
                            } catch ( Exception e) {
                                Validation.addError(paramKey, "validation.notFound", ids[0]);
                            }
                        } else if (ids != null && ids.length > 0 && ids[0].equals("")) {
                            bw.set(field.getName(), o, null);
                            params.remove(paramKey);
                        }
                    }
                }
	    	}
	    	bw.bind(name,  o.getClass(), params, "", o, annotations);
	    	return (T) o;
    	} catch ( ClassNotFoundException e ) {
    		throw new UnexpectedException(e);
    	} catch (Exception e) {
            throw new UnexpectedException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends Model> T edit(String name, Map<String, String[]> params) {
        edit(this, name, params, new Annotation[0]);
        return (T) this;
    }

    public void delete() {
        CassandraPlugin.CassandraModelLoader mf = (CassandraPlugin.CassandraModelLoader)CassandraPlugin.CassandraModelLoader.getFactory(this.getClass());
        mf.delete(this);
    }

    public static long count() {
        throw new UnsupportedOperationException("count: Please annotate your model with @play.modules.cassandra.annotations.Entity annotation.");
    }

    @SuppressWarnings("UnusedParameters")
    public static <T> T get(String id, String field) {
        throw new UnsupportedOperationException("Please annotate your model with @play.modules.cassandra.annotations.Entity annotation.");
    }

    @SuppressWarnings("UnusedParameters")
    public static void set(String id, String field, Object value) {
        throw new UnsupportedOperationException("Please annotate your model with @play.modules.cassandra.annotations.Entity annotation.");
    }

    @SuppressWarnings("UnusedParameters")
    public static boolean increment(String id, String field, BigDecimal amount) {
        throw new UnsupportedOperationException("Please annotate your model with @play.modules.cassandra.annotations.Entity annotation.");
    }

    @SuppressWarnings("UnusedParameters")
    public static boolean increment(String id, String field, long amount) {
        throw new UnsupportedOperationException("Please annotate your model with @play.modules.cassandra.annotations.Entity annotation.");
    }

    @SuppressWarnings("UnusedParameters")
    public static boolean decrement(String id, String field, long amount) {
		throw new UnsupportedOperationException("Please annotate your model with @play.modules.cassandra.annotations.Entity annotation.");
    }

    @SuppressWarnings("UnusedParameters")
    public static <T extends Model> T findById(Object id) {
        throw new UnsupportedOperationException("Please annotate your model with @play.modules.cassandra.annotations.Entity annotation.");
    }

    @SuppressWarnings("UnusedParameters")
    public static <T extends Model> List<T> findByKey(String key, Object value) {
        throw new UnsupportedOperationException("Please annotate your model with @play.modules.cassandra.annotations.Entity annotation.");
    }

    public static CassandraQuery all() {
        throw new UnsupportedOperationException("Please annotate your model with @play.modules.cassandra.annotations.Entity annotation.");
    }

    @SuppressWarnings({"rawtypes", "UnusedParameters"})
    public static <T extends Model> List<T> findAll(){
        throw new UnsupportedOperationException("Please annotate your model with @play.modules.cassandra.annotations.Entity annotation.");
    }
    @SuppressWarnings("UnusedParameters")
    public static <T extends Model> List<T> findAll(String bucket){
		throw new UnsupportedOperationException("Please annotate your model with @play.modules.cassandra.annotations.Entity annotation.");
	}
	
	//TODO: fix in enhancer
	@SuppressWarnings({"rawtypes", "UnusedParameters"})
	public static <T extends Model> List<T> fetch(Class clazz, Type returnType, int start, int end){
		throw new UnsupportedOperationException("Please annotate your model with @play.modules.cassandra.annotations.Entity annotation.");
	}

	@SuppressWarnings({"rawtypes", "UnusedParameters"})
	public static <T extends Model> T find(Class clazz, String key){
		throw new UnsupportedOperationException("Please annotate your model with @play.modules.cassandra.annotations.Entity annotation.");
	}
	@SuppressWarnings("UnusedParameters")
    public static <T extends Model> T find(String bucket, String key){
		throw new UnsupportedOperationException("Please annotate your model with @play.modules.cassandra.annotations.Entity annotation.");
	}	

	@SuppressWarnings("UnusedParameters")
    public static <T extends Model> List<T> jsonToList(String json, Type listType){
		throw new UnsupportedOperationException("Please annotate your model with @play.modules.cassandra.annotations.Entity annotation.");
	}

    /** Interfaces and Annotations **/
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface PrePersist {
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface Fixture {
        String value();

        boolean replaceModels() default false;
    }
}
