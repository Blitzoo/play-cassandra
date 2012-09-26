package play.modules.cassandra;

import java.util.*;

public class RelationList<T extends Model> implements List<T> {
    private ArrayList<Relation<T>> _relations;

    public RelationList() {
        _relations = new ArrayList<Relation<T>>();
    }
    @Override
    public int size() {
        return _relations.size();
    }

    @Override
    public boolean isEmpty() {
        return _relations.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return  _relations.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        return new RelationIterator<T>(_relations);
    }

    @Override
    public Object[] toArray() {
        ArrayList<T> realItems = new ArrayList<T>();
        for ( Relation<T> relation : _relations) {
            try {
                realItems.add(relation.get());
            } catch (ClassNotFoundException e) {
                realItems.add(null);
            }
        }
        return realItems.toArray();
    }

    @Override
    public <T> T[] toArray(T[] ts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(T t) {
        return _relations.add(new Relation<T>(t));
    }

    public boolean add(Relation<T> relation) {
        return _relations.add(relation);
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> objects) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends T> ts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(int i, Collection<? extends T> ts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> objects) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> objects) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        _relations.clear();
    }

    @Override
    public T get(int i) {
        try {
            Relation<T> relation = _relations.get(i);
            return relation.get();
        } catch ( ClassNotFoundException e ) {
            return null;
        }
    }

    @Override
    public T set(int i, T t) {
        _relations.set(i, new Relation<T>(t));
        return t;
    }

    @Override
    public void add(int i, T t) {
        _relations.add(i, new Relation<T>(t));
    }

    @Override
    public T remove(int i) {
        T model = get(i);
        _relations.remove(i);
        return model;
    }

    @Override
    public int indexOf(Object o) {
        return _relations.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return _relations.lastIndexOf(o);
    }

    @Override
    public ListIterator<T> listIterator() {
        return new RelationListIterator<T>(_relations);
    }

    @Override
    public ListIterator<T> listIterator(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<T> subList(int i, int i1) {
        throw new UnsupportedOperationException();
    }
}
