package play.modules.cassandra.util;

import play.Play;
import play.exceptions.UnexpectedException;
import play.modules.cassandra.MapModel;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: mwilson
 * Date: 12-04-11
 * Time: 3:36 PM
 * To change this template use File | Settings | File Templates.
 */
public class SchemaExtractor {
    public static void main(String[] args) throws Exception {
        String filename = "schema-cassandra.txt";
        String outputDir = "conf/";

        // initiate play! framework
        File root = new File(System.getProperty("application.path"));
        Play.init(root, System.getProperty("play.id", ""));
        Thread.currentThread().setContextClassLoader(Play.classloader);
        Class c = Play.classloader.loadClass("play.modules.cassandra.util.SchemaExtractor");
        Method m = c.getMethod("mainWork", String.class, String.class);
        m.invoke(c.newInstance(), filename, outputDir);
        System.exit(0);
    }

    public static void addDatabaseToSchema(PrintStream fout, String databaseName)
    {
        fout.printf("create keyspace %s\n", databaseName);
        fout.println("\twith strategy_options = {replication_factor:3}");
        fout.println("\tand placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy';");
        fout.printf("use %s;\n\n", databaseName);
    }
    
    public static void addEntityToSchema(PrintStream fout, Class entity)
    {
        String family = entity.getName().replaceAll("models\\.", "");
        if (MapModel.class.isAssignableFrom(entity)) {
            fout.printf("create column family %s\n", family);
            fout.println("  with comparator = 'CompositeType(UTF8Type, UTF8Type, UTF8Type)'");
            fout.println("	and key_validation_class = 'UTF8Type'");
            fout.println("	and default_validation_class = 'UTF8Type';");
            return;
        }

        // Regular models continue
        fout.printf("create column family %s;\n", family);
        Field[] fields = entity.getFields();
        Boolean addCountersFamily = false;
        for (Field field : fields) {
            if (field.getAnnotation(play.modules.cassandra.annotations.Counter.class) != null){
                addCountersFamily = true;
            }
            //System.out.println("  " + field.getType().getName() + " : " + field.getName());
        }
        if (addCountersFamily) {
            System.out.println("  Counters found, adding counter family.");
            fout.printf("create column family %s_Counters\n", family);
            fout.println("	with default_validation_class = CounterColumnType");
            fout.println("	and replicate_on_write = true");
            fout.println("	and comparator = UTF8Type;");
        }
    }
    
    public static void mainWork(String filename, String outputDir) throws Exception {
        try {
            File file = new File(outputDir, filename);
            FileOutputStream fileStream = new FileOutputStream (file);
            PrintStream fout = new PrintStream(fileStream);

            String keyspace = Play.configuration.getProperty("cassandra.keyspace");
            if ( null == keyspace ) {
                throw new UnexpectedException("Missing configuration property for cassandra.keyspace");
            }
            SchemaExtractor.addDatabaseToSchema(fout, keyspace);
            
            List<Class> entities = Play.classloader.getAnnotatedClasses(play.modules.cassandra.annotations.Entity.class);
            for ( Class entity : entities ) {
                System.out.println(entity.getName());
                SchemaExtractor.addEntityToSchema(fout, entity);
            }
            System.out.println("Schema saved to " + file.toString());
            fileStream.close();
        } catch (IOException e) {
            System.out.println(e);
        }
    }
}
