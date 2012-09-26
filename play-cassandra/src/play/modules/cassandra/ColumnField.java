package play.modules.cassandra;

import com.netflix.astyanax.mapping.Column;
import com.netflix.astyanax.mapping.Id;
import net.sf.oval.constraint.NotNull;
import play.exceptions.UnexpectedException;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.math.BigDecimal;
import java.util.Collection;

/**
 * Encapsulates a column inside a model.
 *
 * User: mwilson
 * Date: 12-04-26
 * Time: 12:55 PM
 */
public class ColumnField {
    private String _name;
    private Boolean _idField;
    private Boolean _counter;
    private Boolean _decimal;
    private Boolean _isList;
    private Boolean _isIndex;
    private Integer _scale;
    private Class<?> _type;
    private Class<?> _genericType;
    private Boolean _allowNull;
    private Field _sourceField;


    public ColumnField(Field field) {
        _sourceField = field;
        _name = field.getName().toUpperCase();
        _idField = field.isAnnotationPresent(Id.class);
        _counter = field.isAnnotationPresent(play.modules.cassandra.annotations.Counter.class);
        _type = field.getType();
        _genericType = _type;
        _decimal = BigDecimal.class.isAssignableFrom(_type);
        _scale = 0;
        _allowNull = !field.isAnnotationPresent(NotNull.class);
        _isList = Collection.class.isAssignableFrom(_type);
        _isIndex = field.isAnnotationPresent(play.modules.cassandra.annotations.Index.class);

        if ( _idField ) {
            Id annotation = field.getAnnotation(Id.class);
            if ( null != annotation.value() && !annotation.value().trim().isEmpty() ) {
                _name = annotation.value().trim();
            }
        }
        if ( field.isAnnotationPresent(Column.class)) {
            Column annotation = field.getAnnotation(Column.class);
            _name = annotation.value().toUpperCase();
        }

        if ( _isList ) {
            ParameterizedType listType = (ParameterizedType)field.getGenericType();
            _genericType = (Class<?>) listType.getActualTypeArguments()[0];
        }

        if ( _counter && _decimal) {
            try {
                play.modules.cassandra.annotations.Counter counterAnnotation = field.getAnnotation(play.modules.cassandra.annotations.Counter.class);
                _scale = Integer.parseInt(counterAnnotation.scale());
                _allowNull = counterAnnotation.allowNull();
            } catch ( NumberFormatException e ) {
                throw new UnexpectedException("BigDecimal counter field must declare scale for conversion (Use @Counter(scale=2)");
            }
        }
    }

    public Class<?> getType() {
        return _type;
    }

    public Class<?> genericType() {
        return _genericType;
    }

    public Boolean isIdentity() {
        return _idField;
    }

    public Boolean isList() {
        return _isList;
    }

    public Boolean isCounter() {
        return _counter;
    }

    public String getName() {
        return _name;
    }

    public Boolean allowNull() {
        return _allowNull;
    }

    public Object parseValue(Object value) {
        if ( _counter && _decimal && null != value) {
            BigDecimal decimalValue = new BigDecimal((Long)value);
            decimalValue = decimalValue.divide(new BigDecimal(Math.pow(10, _scale)), _scale, BigDecimal.ROUND_HALF_DOWN);
            return decimalValue;
        }

        if ( null == value && !_allowNull) {
            value = new BigDecimal("0.00");
        }

        return value;
    }

    public int getModifiers() {
        return _sourceField.getModifiers();
    }

    public void set(Object o, Object val) throws IllegalAccessException {
        _sourceField.set(o, val);
    }

    public Object get(Object o) throws IllegalAccessException {
        return _sourceField.get(o);
    }

    public long getLong(Object o ) throws IllegalAccessException {
        return _sourceField.getLong(o);
    }

    public boolean getBoolean(Object o ) throws IllegalAccessException {
        return _sourceField.getBoolean(o);
    }

    public double getDouble(Object o) throws IllegalAccessException {
        return _sourceField.getDouble(o);
    }

    public int getInt(Object o) throws IllegalAccessException {
        return _sourceField.getInt(o);
    }


    /**
     * Converts a value to long based on the properties of this column
     *
     * @param value Raw value of column passed from Cassandra
     * @return Value of column converted to Long
     */
    public Long toLong(Object value) {
        Long targetValue = null;
        if ( _decimal ) {
            BigDecimal decimalValue = (BigDecimal)value;
            if ( null != decimalValue) {
                targetValue = decimalValue.multiply(new BigDecimal(Math.pow(10, _scale))).longValue();
            }
        } else if ( Long.class.isAssignableFrom(_type)) {
            targetValue = (Long)value;
        }
        return targetValue;
    }

    public boolean isIndex() {
        return _isIndex;
    }
}
