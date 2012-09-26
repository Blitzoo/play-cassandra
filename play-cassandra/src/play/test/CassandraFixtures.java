package play.test;

import play.db.Model;

import java.util.List;

@SuppressWarnings("WeakerAccess")
public class CassandraFixtures extends Fixtures {
    public static void deleteDatabase() {
    }
    
    public static void delete(Class<? extends Model> ... types) {
    }
    
    public static void delete(List<Class<? extends Model>> classes) {
    }
    
    public static void deleteAllModels() {
    }
}