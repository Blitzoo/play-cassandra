package play.modules.cassandra;

import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import play.classloading.ApplicationClasses.ApplicationClass;
import play.classloading.enhancers.Enhancer;

class CassandraEnhancer extends Enhancer {
    private static final String PACKAGE_NAME = "play.modules.cassandra";

	@Override
	public void enhanceThisClass(ApplicationClass applicationClass)
			throws Exception {
		final CtClass ctClass = makeClass(applicationClass);
        final CtClass modelClass = classPool.getCtClass("play.modules.cassandra.Model");
        final CtClass mappingModelClass = classPool.getCtClass("play.modules.cassandra.MapModel");

        // Classes must be marked with play.modules.cassandra.annotations.Entity annotation
        if ( !this.hasAnnotation(ctClass, play.modules.cassandra.annotations.Entity.class.getName())) return;

        if ( ctClass.subclassOf(modelClass)) {
            enhanceModelClass(ctClass, applicationClass);
        }

        if ( ctClass.subclassOf(mappingModelClass)) {
            enhanceMapModelClass(ctClass, applicationClass);
        }
	}

    /**
     * TODO: Is removeMethod() really appropriate for the getId call?
     * @param ctClass Class reference being modified
     * @param applicationClass Reference to Play! application
     * @throws Exception General exception catch - Play! Framework handles the exception
     */
    private void enhanceModelClass(CtClass ctClass, ApplicationClass applicationClass) throws Exception {
    	String entityName = ctClass.getName();
        CassandraLogger.debug("enhancing %s...", entityName);

        classPool.importPackage(PACKAGE_NAME);

        String className = entityName + ".class";

        CtField mf = CtField.make("protected static play.db.Model.Factory mf = CassandraPlugin.CassandraModelLoader.getFactory(" + className + ");", ctClass);
        ctClass.addField(mf);

        // getFactory
        CtMethod getModelFactory = CtMethod.make("public static play.db.Model.Factory getModelFactory() { return mf; }",ctClass);
        ctClass.addMethod(getModelFactory);

        // Add the ID getter column to the Model
        CtMethod getId = getMethod("getId", ctClass);
        if ( null != getId ) {
            ctClass.removeMethod(getId);
        }
        getId = CtMethod.make("public Object getId() { return mf.keyValue(this);}", ctClass);
        ctClass.addMethod(getId);

        // count
        CtMethod count = CtMethod.make("public static long count() { play.modules.cassandra.CassandraPlugin.CassandraModelLoader cmf = (play.modules.cassandra.CassandraPlugin.CassandraModelLoader)mf;return cmf.count(); }", ctClass);
        ctClass.addMethod(count);

        // increment
        CtMethod incrementBigDecimal = CtMethod.make("public static boolean increment(String id, String field, java.math.BigDecimal amount) { play.modules.cassandra.CassandraPlugin.CassandraModelLoader cmf = (play.modules.cassandra.CassandraPlugin.CassandraModelLoader)mf;return cmf.increment(id, field, amount); }", ctClass);
        ctClass.addMethod(incrementBigDecimal);

        // increment
        CtMethod increment = CtMethod.make("public static boolean increment(String id, String field, long amount) { play.modules.cassandra.CassandraPlugin.CassandraModelLoader cmf = (play.modules.cassandra.CassandraPlugin.CassandraModelLoader)mf;return cmf.increment(id, field, amount); }", ctClass);
        ctClass.addMethod(increment);

        // decrement
        CtMethod decrement = CtMethod.make("public static boolean decrement(String id, String field, long amount) { play.modules.cassandra.CassandraPlugin.CassandraModelLoader cmf = (play.modules.cassandra.CassandraPlugin.CassandraModelLoader)mf;return cmf.increment(id, field, amount*-1); }", ctClass);
        ctClass.addMethod(decrement);

        CtMethod set = CtMethod.make("public static void set(String id, String field, Object value) { play.modules.cassandra.CassandraPlugin.CassandraModelLoader cmf = (play.modules.cassandra.CassandraPlugin.CassandraModelLoader)mf; cmf.setColumnValue(id, field, value); }", ctClass);
        ctClass.addMethod(set);

        CtMethod get = CtMethod.make("public static Object get(String id, String field) { play.modules.cassandra.CassandraPlugin.CassandraModelLoader cmf = (play.modules.cassandra.CassandraPlugin.CassandraModelLoader)mf;return cmf.getColumnValue(id, field); }", ctClass);
        ctClass.addMethod(get);

        // findById
        CtMethod findById = CtMethod.make("public static Model findById(Object id) { return (Model)mf.findById(id); }",ctClass);
        ctClass.addMethod(findById);

        // findByKey
        CtMethod findByKey = CtMethod.make("public static java.util.List findByKey(String key, Object value) { play.modules.cassandra.CassandraPlugin.CassandraModelLoader cmf = (play.modules.cassandra.CassandraPlugin.CassandraModelLoader)mf;return cmf.findByKey(key, value); }",ctClass);
        ctClass.addMethod(findByKey);

        // findAll
        CtMethod findAll = CtMethod.make("public static java.util.List findAll() { play.modules.cassandra.CassandraPlugin.CassandraModelLoader cmf = (play.modules.cassandra.CassandraPlugin.CassandraModelLoader)mf;return cmf.findAll().fetch(); }",ctClass);
        ctClass.addMethod(findAll);

        // all
        CtMethod all = CtMethod.make("public static CassandraQuery all() { play.modules.cassandra.CassandraPlugin.CassandraModelLoader cmf = (play.modules.cassandra.CassandraPlugin.CassandraModelLoader)mf;return cmf.findAll(); }",ctClass);
        ctClass.addMethod(all);

        // #--------------------- BEGIN TIMESTAMP ACCESSORS
        CtMethod getCreated = CtMethod.make("public org.joda.time.DateTime getCreated() { return new org.joda.time.DateTime(this.created); }", ctClass);
        ctClass.addMethod(getCreated);
        CtMethod getUpdated = CtMethod.make("public org.joda.time.DateTime getUpdated() { return new org.joda.time.DateTime(this.updated); }", ctClass);
        ctClass.addMethod(getUpdated);
        // #--------------------- END TIMESTAMP ACCESSORS

        applicationClass.enhancedByteCode = ctClass.toBytecode();
        ctClass.defrost();
    }

    private void enhanceMapModelClass(CtClass ctClass, ApplicationClass applicationClass) throws Exception {
        String columnFamilyName = ctClass.getSimpleName();
        String entityName = ctClass.getName();
        CassandraLogger.debug("enhancing mapmodel class %s...", entityName);

        classPool.importPackage(PACKAGE_NAME);

        // findAll
        CtMethod findAll = CtMethod.make("public static java.util.List findAll(String rowKey ) { return  _findAll( \"" + columnFamilyName + "\", rowKey ); }", ctClass);
        ctClass.addMethod(findAll);

        // getDictionaryKeys
        CtMethod getDictionaryKeys = CtMethod.make("public static java.util.List getDictionaryKeys(String rowKey ) { return  _getDictionaryKeys( \"" + columnFamilyName + "\", rowKey ); }", ctClass);
        ctClass.addMethod(getDictionaryKeys);

        // find
        CtMethod find = CtMethod.make("public static MapModel find(String rowKey, String dictionaryKey) { return  _find( \"" + columnFamilyName + "\", rowKey, dictionaryKey); }", ctClass);
        ctClass.addMethod(find);

        // delete
        CtMethod deleteDictionaryKey = CtMethod.make("public static int delete(String rowKey, String dictionaryKey) { return  _delete( \"" + columnFamilyName + "\", rowKey, dictionaryKey, null); }", ctClass);
        ctClass.addMethod(deleteDictionaryKey);

        // delete subcolumn
        CtMethod deleteValkey = CtMethod.make("public static int delete(String rowKey, String dictionaryKey, String columnKey) { return  _delete( \"" + columnFamilyName + "\", rowKey, dictionaryKey, columnKey); }", ctClass);
        ctClass.addMethod(deleteValkey);

        // writeComposite
        CtMethod writeComposite = CtMethod.make("public static void writeComposite(String rowKey, String dictionaryKey, String key, String value ) { _writeComposite(\"" + columnFamilyName + "\", rowKey, dictionaryKey, key, value, null ); }", ctClass);
        ctClass.addMethod(writeComposite);

        // writeCompositeTtl
        CtMethod writeCompositeTtl = CtMethod.make("public static void writeComposite(String rowKey, String dictionaryKey, String key, String value, Integer ttl ) { _writeComposite(\"" + columnFamilyName + "\", rowKey, dictionaryKey, key, value, ttl ); }", ctClass);
        ctClass.addMethod(writeCompositeTtl);

        applicationClass.enhancedByteCode = ctClass.toBytecode();
        ctClass.defrost();
    }

    @SuppressWarnings("SameParameterValue")
    private CtMethod getMethod(String methodName, CtClass ctClass) {
        for ( CtMethod cm: ctClass.getDeclaredMethods()) {
            if ( methodName.equals(cm.getName()) && cm.getDeclaringClass().equals(ctClass)) {
                // User has defined get ID already
                return cm;
            }
        }
        return null;
    }
}
