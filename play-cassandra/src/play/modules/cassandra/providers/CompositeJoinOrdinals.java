package play.modules.cassandra.providers;

import com.netflix.astyanax.annotations.Component;

public class CompositeJoinOrdinals {
    @Component(ordinal=0) String dictionaryName;
    @Component(ordinal=1) String key;

    public CompositeJoinOrdinals() { }
}
