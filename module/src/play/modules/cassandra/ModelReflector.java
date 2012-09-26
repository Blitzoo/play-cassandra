package play.modules.cassandra;

import com.netflix.astyanax.mapping.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import play.Play;

import javax.persistence.Transient;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

/**
 * Provides reflection of Cassandra models for consumption by
 * data provider classes.
 *
 * User: mwilson
 * Date: 12-04-26
 * Time: 10:37 AM
 */
public class ModelReflector {
    private static final Map<String, ModelReflector> classMap = new HashMap<String,ModelReflector>();
    private Class<? extends Model> clazz;
    private final ColumnFamily<String, String> _columnFamily;
    private final ColumnFamily<String, String> _counterColumnFamily;

    /**
     * Allow instantiation without a class object - we can infer it
     * from the class name
     *
     * @param className Name of class being reflected
     * @return Model Reflector for class
     */
    @SuppressWarnings("unchecked")
    static public ModelReflector reflectorFor(String className) {
        try {
            String vType = className;
            if (!vType.startsWith("models.")) {
                vType = "models." + vType;
            }
            Class<? extends Model> vClazz = (Class<? extends Model>) Play.classloader.loadClass(vType);
            if ( !classMap.containsKey(vClazz.getName()) ) {
                classMap.put(vClazz.getName(), new ModelReflector(vClazz));
            }
            return classMap.get(vClazz.getName());
        } catch ( ClassNotFoundException e ) {
            return  null;
        }
    }

    static public ModelReflector reflectorFor(Class<?> clazz) {
        return reflectorFor(clazz.getName());
    }

    private ModelReflector(Class<? extends Model> clazz) {
        String ksName = String.format("%s_Counters", clazz.getSimpleName());
        this.clazz = clazz;
        this._columnFamily = new ColumnFamily<String, String>(
                clazz.getSimpleName(),
                StringSerializer.get(),
                StringSerializer.get()
        );
        this._counterColumnFamily = new ColumnFamily<String,String>(
                        ksName,
                        StringSerializer.get(),
                        StringSerializer.get()
                );


    }

    public Class<? extends Model> getType() {
        return this.clazz;
    }

    /**
     * Given the model field's name for the column, return the DB internall column name
     *
     * TODO: Cache the results of this so we don't have to repeat the check every time
     *
     * @param columnName Model-class name of column being queried
     * @return Database-friendly name of column; null if column not found in model
     */
    public ColumnField getColumnField(String columnName) {
        columnName = columnName.toUpperCase();
        for (ColumnField field : getColumnFields() ) {
            if ( field.getName().equals(columnName)) {
                return field;
            }
        }
        return null;
    }

    public Set<ColumnField> getIndexFields() {
        Set<ColumnField> fields = getColumnFields();
        Set<ColumnField> indexFields = new HashSet<ColumnField>();
        for ( ColumnField field : fields ) {
            if ( field.isIndex() ) {
                indexFields.add(field);
            }
        }
        return indexFields;
    }

    /**
     * TODO: Cache the result of this
     * @return
     */
    public String getIdentityField() {
        for ( ColumnField field : getColumnFields() ) {
            if ( field.isIdentity()) {
                return field.getName();
            }
        }
        return null;
    }

    public Field getFieldByName(String fieldName) {
        String ucFieldName = fieldName.toUpperCase();
        for ( Field field : getPublicFields() ) {
            ColumnField columnField = buildColumnField(field);
            if (field.getName().toUpperCase().equals(ucFieldName) || columnField.getName().equals(ucFieldName)) {
                return field;
            }
        }
        return null;
    }
    private Set<Field> getPublicFields() {
        Set<Field> fields = new HashSet<Field>();
        Class<?> columnClazz = this.clazz;
        while (!columnClazz.equals(Object.class)) {
            Collections.addAll(fields, columnClazz.getDeclaredFields());
            columnClazz = columnClazz.getSuperclass();
        }
        // Drop private fields
        for ( Iterator<Field> iter = fields.iterator(); iter.hasNext(); ) {
            Field field = iter.next();
            if ( !Modifier.isPublic(field.getModifiers())) {
                iter.remove();
            }
        }
        return fields;
    }

    public Set<ColumnField> getColumnFields() {
        Set<ColumnField> fields = new HashSet<ColumnField>();
        for ( Field field : getPublicFields()) {
            // Transient fields are not considered column fields
            if (null == field.getAnnotation(Transient.class)) {
                fields.add(buildColumnField(field));
            }
        }
        return fields;
    }

    public Boolean hasCounters() {
        return getCounterColumnNames().size() > 0;
    }

    // TODO: Column name is NOT DRY
    public List<String> getCounterColumnNames() {
        ArrayList<String> counters = new ArrayList<String>();
        for ( Field field : clazz.getFields() ) {
            if ( Modifier.isStatic(field.getModifiers()) ) continue;

            if ( null != field.getAnnotation(play.modules.cassandra.annotations.Counter.class)) {
                String columnName = field.getName().toUpperCase();
                Column columnAnnotation = field.getAnnotation(Column.class);
                if ( null != columnAnnotation && null != columnAnnotation.value() ) {
                    columnName = columnAnnotation.value().toString();
                }
                counters.add(columnName);
            }
        }
        return counters;
    }

    /**
     * TODO: This ignores the ANNOTATION, fix it in the factory
     *
     * @return Column family reference used to access class in Cassandra data store
     */
    public ColumnFamily<String,String> getColumnFamily() {
        return _columnFamily;
    }

    public ColumnFamily<String, String> getCounterColumnFamily() {
        return _counterColumnFamily;
    }

    public ColumnField buildColumnField(Field field) {
        ColumnField columnField = new ColumnField(field);
        return columnField;
    }
}
