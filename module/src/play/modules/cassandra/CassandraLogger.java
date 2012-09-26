package play.modules.cassandra;

import play.Logger;

/**
 * Logger for CassandraPlugin wraps plugin-idendity data around the default Play! Logger
 *
 * User: mwilson
 * Date: 12-03-30
 * Time: 9:10 AM
 */
public class CassandraLogger {
    public static void trace(String msg, Object... args) {
        Logger.trace(msg_(msg, args));
    }

    public static void debug(String msg, Object... args) {
        Logger.debug(msg_(msg, args));
    }

    public static void debug(Throwable t, String msg, Object... args) {
        Logger.debug(t, msg_(msg, args));
    }

    public static void info(String msg, Object... args) {
        Logger.info(msg_(msg, args));
    }

    public static void info(Throwable t, String msg, Object... args) {
        Logger.info(t, msg_(msg, args));
    }

    private static String msg_(String msg, Object... args) {
        return String.format("CassandraPlugin-" + CassandraPlugin.VERSION + "> %1$s",
                String.format(msg, args));
    }

    public static void slow(String msg, Object... args) {
        Logger.debug(msg, msg_(msg, args));
    }

    public static void warn(String msg, Object... args) {
        Logger.warn(msg_(msg, args));
    }

    public static void warn(Throwable t, String msg, Object... args) {
        Logger.warn(t, msg_(msg, args));
    }

    public static void error(String msg, Object... args) {
        Logger.error(msg_(msg, args));
    }

    public static void error(Throwable t, String msg, Object... args) {
        Logger.error(t, msg_(msg, args));
    }
}
