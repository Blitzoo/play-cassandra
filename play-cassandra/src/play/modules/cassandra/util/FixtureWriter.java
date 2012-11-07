package play.modules.cassandra.util;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import play.Play;
import play.exceptions.JPAException;
import play.exceptions.UnexpectedException;
import play.modules.cassandra.*;
import play.modules.cassandra.providers.CassandraSharedHelpers;
import play.test.Fixtures;
import play.vfs.VirtualFile;

import java.io.File;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is intended to parse through all of the models in the database,
 * determine which ones have fixtures that can/should be applied, and push
 * them into the database.
 *
 * User: mwilson
 * Date: 12-04-11
 * Time: 3:36 PM
 */
public class FixtureWriter {
    AstyanaxContext<Cluster> _clusterContext;

    public static void main(String[] args) throws Exception {
        // initiate play! framework
        File root = new File(System.getProperty("application.path"));
        //System.setProperty("application.mode", "dev");
        Play.init(root, System.getProperty("play.id", ""));
        VirtualFile appRoot = VirtualFile.open(Play.applicationPath);
        Play.javaPath.add(0, appRoot.child("conf"));
        Thread.currentThread().setContextClassLoader(Play.classloader);
        FixtureWriter writer = new FixtureWriter();
        writer.run();
        System.exit(0);
    }

    public FixtureWriter()
    {
        String cluster = Play.configuration.getProperty("cassandra.cluster", "PlayCluster");
        _clusterContext = new AstyanaxContext.Builder()
                .forCluster(cluster)
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl())
                .withConnectionPoolConfiguration(CassandraSharedHelpers.createConnectionPoolConfiguration())
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildCluster(ThriftFamilyFactory.getInstance());
    }

    // TODO: Change replication factor in settings
    private void createKeyspaceDefinition(Cluster cluster, String keyspaceName) throws ConnectionException {
        Map<String, String> stratOptions = new HashMap<String, String>();
        stratOptions.put("replication_factor", "1");

        KeyspaceDefinition ksDef = cluster.makeKeyspaceDefinition();
        ksDef.setName(keyspaceName)
                .setStrategyOptions(stratOptions)
                .setStrategyClass("SimpleStrategy");
        cluster.addKeyspace(ksDef);
        CassandraLogger.info("Created keyspace %s", keyspaceName);
    }
    
    private void addColumnFamily(Cluster cluster, String keyspace, Class entity) throws ConnectionException {
        String family = entity.getName().replaceAll("models\\.", "");
        String entitySimpleName = entity.getSimpleName();

        if (MapModel.class.isAssignableFrom(entity)) {
            addMapModelFamily(cluster, keyspace, family, entitySimpleName);
        }

        createOrUpdateColumnFamily(cluster, keyspace, family, entitySimpleName);
        createOrUpdateCounterFamily(cluster, keyspace, entity, family);
    }

    private void createOrUpdateCounterFamily(Cluster cluster, String keyspace, Class entity, String family) throws ConnectionException {
        // Regular models continue
        Field[] fields = entity.getFields();
        Boolean addCountersFamily = false;
        for (Field field : fields) {
            if (field.getAnnotation(play.modules.cassandra.annotations.Counter.class) != null){
                addCountersFamily = true;
            }
        }
        if (addCountersFamily) {
            String counterFamily = String.format("%s_Counters", family);
            if ( null != cluster.getKeyspace(keyspace).describeKeyspace().getColumnFamily(counterFamily)) {
                CassandraLogger.info("Counter family: %s already exists", family);
                return;
            }
            cluster.addColumnFamily(
                    cluster.makeColumnFamilyDefinition()
                            .setName(counterFamily)
                            .setKeyspace(keyspace)
                            .setDefaultValidationClass("CounterColumnType")
                            .setComparatorType("UTF8Type")
                            .setReplicateOnWrite(true)
            );
            CassandraLogger.info("Added counter family: %s", family);
        }
    }

    private void createOrUpdateColumnFamily(Cluster cluster, String keyspace, String family, String entitySimpleName) throws ConnectionException {
        ColumnFamilyDefinition cfDef = cluster.getKeyspace(keyspace).describeKeyspace().getColumnFamily(family);
        if ( null != cfDef) {
            CassandraLogger.info("Cassandra.Model family: %s already exists", family);

            boolean updateRequired = false;
            List<ColumnDefinition> columnDefinitions = cfDef.getColumnDefinitionList();
            for ( ColumnField indexField : ModelReflector.reflectorFor(family).getIndexFields()) {
                CassandraLogger.info("Checking INDEX for field %s", indexField.getName());
                boolean foundIndexField = false;
                for ( ColumnDefinition columnDefinition : columnDefinitions ) {
                    if (columnDefinition.getName().equals(indexField.getName())) {
                        foundIndexField = true;
                        break;
                    }
                }
                if ( !foundIndexField ) {
                    CassandraLogger.info("Not found! Adding INDEX for field %s", indexField.getName());
                    cfDef.addColumnDefinition(cfDef.makeColumnDefinition()
                            .setName(indexField.getName())
                            .setValidationClass("UTF8Type")
                            .setIndex(String.format("%s_%s_index", entitySimpleName, indexField.getName().toLowerCase()), "KEYS")
                    );
                    updateRequired = true;
                }
            }
            if ( updateRequired ) {
                cluster.updateColumnFamily(cfDef);
            }
        } else {
            CassandraLogger.info("Creating Model family: %s", family);
            cfDef = cluster.makeColumnFamilyDefinition()
                                                .setName(family)
                                                .setKeyspace(keyspace)
                                                .setComparatorType("UTF8Type")
                                                .setKeyValidationClass("UTF8Type");

            for ( ColumnField indexField : ModelReflector.reflectorFor(family).getIndexFields()) {
                CassandraLogger.info("Adding INDEX for field %s", indexField.getName());
                cfDef.addColumnDefinition(cfDef.makeColumnDefinition()
                        .setName(indexField.getName())
                        .setValidationClass("UTF8Type")
                        .setIndex(String.format("%s_%s_index", entitySimpleName, indexField.getName().toLowerCase()), "KEYS")
                );
            }

            cluster.addColumnFamily(cfDef);
        }
    }

    private void addMapModelFamily(Cluster cluster, String keyspace, String family, String entitySimpleName) throws ConnectionException {
        ColumnFamilyDefinition cfDef = cluster.getKeyspace(keyspace).describeKeyspace().getColumnFamily(family);
        if ( null != cfDef) {
            // If the column family already exists, ensure any new index fields are added to the metadata
            CassandraLogger.info("MapModel family: %s already exists", family);

            boolean updateRequired = false;
            List<ColumnDefinition> columnDefinitions = cfDef.getColumnDefinitionList();
            for ( ColumnField indexField : ModelReflector.reflectorFor(family).getIndexFields()) {
                CassandraLogger.info("Checking INDEX for field %s", indexField.getName());
                boolean foundIndexField = false;
                for ( ColumnDefinition columnDefinition : columnDefinitions ) {
                    if (columnDefinition.getName().equals(indexField.getName())) {
                        foundIndexField = true;
                        break;
                    }
                }
                if ( !foundIndexField ) {
                    CassandraLogger.info("Not found! Adding INDEX for field %s", indexField.getName());
                    cfDef.addColumnDefinition(cfDef.makeColumnDefinition()
                            .setName(indexField.getName())
                            .setValidationClass("UTF8Type")
                            .setIndex(String.format("%s_%s_index", entitySimpleName, indexField.getName().toLowerCase()), "KEYS")
                    );
                    updateRequired = true;
                }
            }
            if ( updateRequired ) {
                cluster.updateColumnFamily(cfDef);
            }
        } else {
            // If the column Family does not yet exist, create it and all necessary indexes
            CassandraLogger.info("Creating MapModel family: %s", family);
            cfDef = cluster.makeColumnFamilyDefinition()
                    .setName(family)
                    .setKeyspace(keyspace)
                    .setComparatorType("CompositeType(UTF8Type, UTF8Type, UTF8Type)")
                    .setKeyValidationClass("UTF8Type")
                    .setDefaultValidationClass("UTF8Type");

            for ( ColumnField indexField : ModelReflector.reflectorFor(family).getIndexFields()) {
                CassandraLogger.info("Adding INDEX for field %s", indexField.getName());
                cfDef.addColumnDefinition(cfDef.makeColumnDefinition()
                        .setName(indexField.getName())
                        .setValidationClass("UTF8Type")
                        .setIndex(String.format("%s_%s_index", entitySimpleName, indexField.getName().toLowerCase()), "KEYS")
                );
            }
            cluster.addColumnFamily(cfDef);
        }
        return;
    }

    public void run() throws Exception {
        String keyspace = Play.configuration.getProperty("cassandra.keyspace");
        if ( null == keyspace ) {
            throw new UnexpectedException("Missing configuration property for cassandra.keyspace");
        }

        _clusterContext.start();
        Cluster cluster = _clusterContext.getEntity();
        if ( null == cluster.describeKeyspace(keyspace)) {
            CassandraLogger.info("Could not find keyspace %s: Creating it now", keyspace);
            createKeyspaceDefinition(cluster, keyspace);
        } else {
            CassandraLogger.info("Keyspace %s exists.", keyspace);
        }

        List<Class> entities = Play.classloader.getAnnotatedClasses(play.modules.cassandra.annotations.Entity.class);
        for ( Class entity : entities ) {
            addColumnFamily(cluster, keyspace, entity);
            Model.Fixture fixture = (Model.Fixture) entity.getAnnotation(Model.Fixture.class);
            if ( null != fixture) {
                try {
                    if ( fixture.replaceModels() ) {
                        CassandraLogger.info("Deleting existing entries for %s", entity.getSimpleName());
                        Fixtures.delete(entity);
                    }
                    CassandraLogger.info("Loading fixtures for %s", entity.getSimpleName());
                    Fixtures.loadModels(String.format("%s", fixture.value()));
                } catch ( JPAException e ) {
                    CassandraLogger.warn("Received JPA error: " + e.getMessage());
                }
            }
        }
        _clusterContext.shutdown();
    }
}
