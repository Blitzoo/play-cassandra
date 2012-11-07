package models;

import com.netflix.astyanax.mapping.Column;
import com.netflix.astyanax.mapping.Id;
import play.modules.cassandra.Model;
import play.modules.cassandra.annotations.Entity;

@Entity(columnFamily = "GAME")
public class Game extends Model {
    @Id("GUID")
    public String guid;

    @Column("name")
    public String name;
}
