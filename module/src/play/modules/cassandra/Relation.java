package play.modules.cassandra;

import play.Play;

/**
 * Implements a proxy class for Cassandra-based model relations.
 * This is intended to provide
 *
 * User: mwilson
 * Date: 12-04-11
 * Time: 2:59 PM
 */
public class Relation<T extends Model> {
    private Object _id;
    private T _model;
    private String _className;

    public Relation(String className, String id) {
        _id = id;
        _model = null;
        _className = className;
    }

    public Relation(T t) {
        _className = t.getClass().getName();
        _id = t.getId();
        _model = t;
    }

    public T get() throws ClassNotFoundException {
        if ( null == _model ) {
            Class<T> vClazz = (Class<T>)Play.classloader.loadClass(_className);

            _model = (T) play.db.Model.Manager.factoryFor(vClazz).findById(_id);
        }

        return _model;
    }
}
