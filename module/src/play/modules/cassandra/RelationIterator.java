package play.modules.cassandra;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created with IntelliJ IDEA.
 * User: mwilson
 * Date: 12-04-11
 * Time: 3:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class RelationIterator<T extends Model> implements Iterator<T> {
    private int _current;
    private ArrayList<Relation<T>> _relations;
    public RelationIterator(ArrayList<Relation<T>> relations) {
        _current = 0;
        _relations = relations;
    }

    @Override
    public boolean hasNext() {
        if ( _relations.isEmpty() ) {
            return false;
        }

        if ( (_current+1) <= _relations.size() ) {
            return true;
        }
        return false;
    }

    @Override
    public T next() {
        if ( hasNext() ) {
            Relation<T> relation = _relations.get(_current++);
            try {
                return relation.get();
            } catch (ClassNotFoundException e) {
                return null;
            }
        }
        throw new NoSuchElementException(); // iteration has no more elements
    }

    @Override
    public void remove() {
        if ( _current == 0 ) {
            throw new IllegalStateException(); // the next method has not yet been called
        }
        throw new UnsupportedClassVersionError();
    }
}
