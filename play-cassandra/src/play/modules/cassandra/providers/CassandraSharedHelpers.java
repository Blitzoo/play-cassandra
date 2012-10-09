package play.modules.cassandra.providers;

import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import play.Play;
import play.modules.cassandra.CassandraLogger;

@SuppressWarnings("unchecked")
public class CassandraSharedHelpers
{

    public static ConnectionPoolConfigurationImpl createConnectionPoolConfiguration() {
        Integer maxConnsPerHost = Integer.parseInt(Play.configuration.getProperty("cassandra.maxConnsPerHost", "50"));
        String seeds = Play.configuration.getProperty("cassandra.seeds", "127.0.0.1:9160");
        int seedPort = Integer.parseInt(Play.configuration.getProperty("cassandra.seedPort", "9160"));
        CassandraLogger.info("Connecting to seed: %s", seeds);
        return new ConnectionPoolConfigurationImpl("MyConnectionPool")
                .setMaxConnsPerHost(maxConnsPerHost)
                .setSeeds(seeds);
    }
}