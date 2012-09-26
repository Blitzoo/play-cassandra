package play.modules.cassandra;

import java.util.ArrayList;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 * Created with IntelliJ IDEA.
 * User: mwilson
 * Date: 12-05-15
 * Time: 2:01 PM
 * To change this template use File | Settings | File Templates.
 */
public class RelationListIterator<T extends Model> implements ListIterator<T> {
    private int _current;
    private ArrayList<Relation<T>> _relations;
    public RelationListIterator(ArrayList<Relation<T>> relations) {
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
    public boolean hasPrevious() {
        if ( _relations.isEmpty() ) {
            return false;
        }

        return _current > 0;
    }

    @Override
    public T previous() {
        if ( hasPrevious() ) {
            Relation<T> relation = _relations.get(--_current);
            try {
                return relation.get();
            } catch ( ClassNotFoundException e) {
                return null;
            }
        }
        throw new NoSuchElementException(); // iteration has no more elements
    }

    @Override
    public int nextIndex() {
        if ( hasNext() ) {
            return _current+1;
        }

        return _relations.size();
    }

    @Override
    public int previousIndex() {
        return _current-1;
    }

    @Override
    public void remove() {
        if ( _current == 0 ) {
            throw new IllegalStateException(); // the next method has not yet been called
        }
        throw new UnsupportedClassVersionError();
    }

    @Override
    public void set(T t) {
        _relations.set(_current, new Relation<T>(t));
    }

    @Override
    public void add(T t) {
        _relations.add(_current, new Relation<T>(t));
    }
}
